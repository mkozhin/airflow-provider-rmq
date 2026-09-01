"""Tests for the plugin module: what it registers with Airflow, the Subscriptions
page access it gives the Op role at webserver start, and the switch that governs it.
"""
from __future__ import annotations

import contextlib
import logging
import os
from types import SimpleNamespace
from unittest.mock import patch

import flask
import pytest

from airflow_provider_rmq.watcher import tunables
from airflow_provider_rmq.watcher.plugin import (
    _OP_PERMISSIONS,
    RMQWatcherPlugin,
    _bp,
    _grant_op_access,
)
from airflow_provider_rmq.watcher.tunables import (
    GRANT_OP_ACCESS_OPTION,
    GRANT_OP_ACCESS_SECTION,
    read_flag,
)


def test_plugin_name():
    assert RMQWatcherPlugin.name == "rmq_watcher"


def test_plugin_has_listener():
    assert len(RMQWatcherPlugin.listeners) == 1
    assert type(RMQWatcherPlugin.listeners[0]).__name__ == "RMQWatcherListener"


def test_plugin_has_appbuilder_view():
    views = RMQWatcherPlugin.appbuilder_views
    assert len(views) == 1
    assert views[0]["name"] == "Subscriptions"
    assert views[0]["category"] == "RabbitMQ"
    assert type(views[0]["view"]).__name__ == "RMQWatcherView"


def test_plugin_has_blueprint_with_templates():
    import os
    bps = RMQWatcherPlugin.flask_blueprints
    assert len(bps) == 1
    bp = bps[0]
    assert bp.name == "rmq_watcher"
    for tmpl in ["subscriptions.html", "subscription_form.html"]:
        path = os.path.join(bp.template_folder, "rmq_watcher", tmpl)
        assert os.path.isfile(path), f"template missing from blueprint: {tmpl}"


def test_rmq_trigger_importable_from_watcher():
    from airflow_provider_rmq.watcher import rmq_trigger
    assert callable(rmq_trigger)


def test_on_load_does_not_raise_on_ensure_table_error():
    """P1: исключение в ensure_table_exists не должно всплывать из on_load."""
    with patch(
        "airflow_provider_rmq.watcher.plugin.ensure_table_exists",
        side_effect=Exception("DB unavailable"),
    ):
        RMQWatcherPlugin.on_load()  # не должен бросить исключение


class _FakeRole:
    def __init__(self, name: str):
        self.name = name
        self.permissions: set = set()


class _FakeSession:
    """The session FAB shares with the role synchronisation that follows the callback.

    :param raises: What ``rollback`` throws, for a database still gone by the time the
        handler tries to clear the transaction.
    """

    def __init__(self, raises=None):
        self.raises = raises
        self.rollbacks = 0

    def rollback(self):
        self.rollbacks += 1
        if self.raises is not None:
            raise self.raises


class _ExpiredPermissions:
    """A role's permission collection as it reads once FAB rolled a failed commit back.

    The rollback expires the role, so the membership test the callback runs next re-emits
    a SELECT against the database that has just refused the commit — which is where this
    raises.
    """

    def __contains__(self, item):
        raise RuntimeError("current transaction is aborted")


class _FakeSecurityManager:
    """Enough of the FAB security manager for the grant: roles, permissions, both edges.

    ``create_permission`` returns the pair itself, so a role's ``permissions`` set reads
    as the action-resource pairs the callback actually handed it, and it is the only way
    a pair comes to exist — ``get_permission`` answers ``None`` for one nobody made.
    ``add_permission_to_role`` skips a pair the role already holds, the way FAB's own
    does, so a repeated grant is visible as an empty ``added`` list rather than assumed.

    :param permissions: Pairs the role holds at the start, which are also pairs FAB
        knows: a role cannot hold one that does not exist.
    :param refuses: Pairs whose creation answers ``None``, as FAB's does on a database
        that refuses the insert.
    :param rolls_back: Pairs whose add and remove leave the role exactly as it was and
        say nothing about it, the way FAB's do when the commit inside them fails: both
        methods log the error, roll the session back and return.
    :param session_raises: What the session's ``rollback`` throws, ``None`` for one that
        works. ``get_session`` is a plain attribute here and a property on FAB's manager;
        the callback reads it the same way either way.
    """

    def __init__(
        self, roles=("Op",), permissions=(), refuses=(), rolls_back=(), session_raises=None
    ):
        self.get_session = _FakeSession(session_raises)
        self.roles = {name: _FakeRole(name) for name in roles}
        for role in self.roles.values():
            role.permissions.update(permissions)
        self.known: set = set(permissions)
        self.refuses: set = set(refuses)
        self.rolls_back: set = set(rolls_back)
        self.created: list = []
        self.added: list = []
        self.removed: list = []

    def find_role(self, name):
        return self.roles.get(name)

    def create_permission(self, action, resource):
        self.created.append((action, resource))
        if (action, resource) in self.refuses:
            return None
        self.known.add((action, resource))
        return (action, resource)

    def get_permission(self, action, resource):
        return (action, resource) if (action, resource) in self.known else None

    def add_permission_to_role(self, role, permission):
        if permission in role.permissions:
            return
        self.added.append(permission)
        if permission in self.rolls_back:
            return
        role.permissions.add(permission)

    def remove_permission_from_role(self, role, permission):
        self.removed.append(permission)
        if permission in self.rolls_back:
            return
        role.permissions.discard(permission)


def _state(sm):
    """A stand-in for the blueprint setup state the callback is handed."""
    return SimpleNamespace(app=SimpleNamespace(appbuilder=SimpleNamespace(sm=sm)))


class TestOpAccessGrant:
    """The provider hands the Subscriptions page to the Op role at webserver start."""

    def test_registering_the_blueprint_runs_the_grant(self):
        """The callback is wired to the blueprint, not merely callable.

        Every other test here calls the function itself and would stay green with the
        registration gone, leaving the grant to never happen on a live webserver. The
        arguments of the read are recorded here too: they name the configuration option
        an operator is told to set, and no other test reads that name off the call.
        """
        sm = _FakeSecurityManager()
        app = flask.Flask(__name__)
        app.appbuilder = SimpleNamespace(sm=sm)
        reads: list = []

        def record(section, option, default):
            reads.append((section, option, default))
            return True

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", record):
            app.register_blueprint(_bp)

        assert reads == [("rmq_watcher", "grant_op_access", True)]
        assert sm.roles["Op"].permissions == set(_OP_PERMISSIONS)

    def test_the_six_expected_pairs_are_granted(self):
        sm = _FakeSecurityManager()

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        assert set(sm.added) == {
            ("can_read", "RMQ Subscriptions"),
            ("can_create", "RMQ Subscriptions"),
            ("can_edit", "RMQ Subscriptions"),
            ("can_delete", "RMQ Subscriptions"),
            ("menu_access", "Subscriptions"),
            ("menu_access", "RabbitMQ"),
        }
        assert not sm.removed

    def test_the_pairs_name_the_resources_the_page_is_registered_under(self):
        """The grant and the menu registration have to name the same three strings.

        A resource FAB does not know refuses no request and hides no menu entry: the
        grant would report success and the role would still see nothing.
        """
        view = RMQWatcherPlugin.appbuilder_views[0]
        assert view["name"] == "Subscriptions"
        assert view["category"] == "RabbitMQ"
        assert type(view["view"]).class_permission_name == "RMQ Subscriptions"

        assert set(_OP_PERMISSIONS) == {
            ("can_read", "RMQ Subscriptions"),
            ("can_create", "RMQ Subscriptions"),
            ("can_edit", "RMQ Subscriptions"),
            ("can_delete", "RMQ Subscriptions"),
            ("menu_access", "Subscriptions"),
            ("menu_access", "RabbitMQ"),
        }

    def test_granting_again_over_the_same_role_adds_nothing_new(self):
        """A second start over a role that already holds them changes nothing.

        The webserver starts as often as it is restarted, and every start walks the same
        six pairs: an add that repeats itself would write six rows a start.
        """
        sm = _FakeSecurityManager(permissions=_OP_PERMISSIONS)

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        assert sm.added == []
        assert sm.roles["Op"].permissions == set(_OP_PERMISSIONS)

    def test_a_false_flag_takes_the_permissions_back(self):
        """The switch governs the access, not just the grant.

        After one start on the default the permissions are in the database, and FAB
        removes a valid role-permission pair in neither ``bulk_sync_roles`` nor
        ``clean_perms``. A role that already holds them is therefore the only state in
        which the revocation can be seen at all.
        """
        sm = _FakeSecurityManager(permissions=_OP_PERMISSIONS)

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=False):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set()
        assert set(sm.removed) == set(_OP_PERMISSIONS)
        assert not sm.added

    def test_a_false_flag_over_a_role_without_them_is_quiet(self, caplog):
        """Taking away what nobody holds creates nothing.

        A pair FAB does not know is a pair no role holds, so the revocation asks for the
        existing one instead of making it: an installation that has never granted the
        access ends the start with as empty a permission table as it began. Such a pair
        counts as one the role is without, which is what the summary line reports.
        """
        sm = _FakeSecurityManager()

        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=False):
            _grant_op_access(_state(sm))

        messages = [r.getMessage() for r in caplog.records]
        assert sm.roles["Op"].permissions == set()
        assert not sm.created
        assert not sm.removed
        assert not sm.added
        assert any("is without 6 of the 6" in m for m in messages), messages

    def test_a_missing_op_role_is_a_warning(self, caplog):
        """A virgin database has no Op role yet: it is created after this callback."""
        sm = _FakeSecurityManager(roles=())

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        assert not sm.created
        assert any("Op" in r.getMessage() for r in caplog.records), [
            r.getMessage() for r in caplog.records
        ]

    def test_a_security_manager_that_raises_does_not_stop_the_webserver(self, caplog):
        sm = _FakeSecurityManager()
        sm.find_role = lambda name: (_ for _ in ()).throw(RuntimeError("no database"))

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        assert any("Op" in r.getMessage() for r in caplog.records), [
            r.getMessage() for r in caplog.records
        ]

    def test_a_read_that_breaks_outright_leaves_the_role_as_it_is(self, caplog):
        """A reader that raises grants nothing and revokes nothing.

        Every value the reader can be handed has an answer — a yes, a no, or the no it
        gives whatever it cannot read — so a raise coming out of it is the reader
        itself breaking, and the callback then has no answer of any kind to act on. It
        touches nothing and says so in the log, which is where the six permissions the
        role holds or does not hold stay exactly as the last start left them.
        """
        sm = _FakeSecurityManager(permissions=_OP_PERMISSIONS)

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.plugin"
        ), patch(
            "airflow_provider_rmq.watcher.plugin.read_flag",
            side_effect=RuntimeError("no database"),
        ):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set(_OP_PERMISSIONS)
        assert not sm.created
        assert not sm.added
        assert not sm.removed
        assert any(
            GRANT_OP_ACCESS_OPTION in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_a_permission_the_database_refuses_is_named_and_the_rest_go_on(self, caplog):
        """FAB answers a refused insert with ``None`` rather than an exception.

        Handing that to ``add_permission_to_role`` would raise and cost the pairs behind
        it, and passing it over silently would leave the page half-open with a line in
        the log saying it was opened.
        """
        refused = ("menu_access", "RabbitMQ")
        sm = _FakeSecurityManager(refuses=[refused])

        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        messages = [r.getMessage() for r in caplog.records]
        assert set(sm.added) == set(_OP_PERMISSIONS) - {refused}
        assert any("RabbitMQ" in m for m in messages), messages
        assert any("holds 5 of the 6" in m for m in messages), messages

    def test_a_grant_the_database_rolled_back_is_not_reported_as_done(self, caplog):
        """FAB's role-permission calls answer nothing and raise nothing.

        A commit they could not make is logged inside them, rolled back and returned
        from exactly as a successful one is, so a callback counting its own calls would
        report a page opened to a role that holds nothing of it. What the role holds
        when the call comes back is the only account of the write there is.
        """
        stuck = ("can_delete", "RMQ Subscriptions")
        sm = _FakeSecurityManager(rolls_back=[stuck])

        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        messages = [r.getMessage() for r in caplog.records]
        assert sm.roles["Op"].permissions == set(_OP_PERMISSIONS) - {stuck}
        assert any(
            "can_delete" in m and "was not given to" in m for m in messages
        ), messages
        assert any("holds 5 of the 6" in m for m in messages), messages

    def test_a_revocation_the_database_rolled_back_is_not_reported_as_done(self, caplog):
        """The log line is all an administrator has to go on when he withdraws access.

        He sets the switch to false to take create, edit and delete on subscriptions
        away from the role, and a summary counting calls would tell him it happened
        while the role still held every one of them.
        """
        stuck = ("can_edit", "RMQ Subscriptions")
        sm = _FakeSecurityManager(permissions=_OP_PERMISSIONS, rolls_back=[stuck])

        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=False):
            _grant_op_access(_state(sm))

        messages = [r.getMessage() for r in caplog.records]
        assert sm.roles["Op"].permissions == {stuck}
        assert any(
            "can_edit" in m and "was not taken from" in m for m in messages
        ), messages
        assert any("is without 5 of the 6" in m for m in messages), messages

    def test_a_landed_check_that_raises_leaves_the_session_rolled_back(self, caplog):
        """The callback shares FAB's session with the role synchronisation behind it.

        A database fault inside the loop leaves that session's transaction aborted, and
        Airflow's own ``sync_roles`` — which runs on it moments later, in the same
        ``create_app`` — raises on the aborted transaction even once the database is
        back, so a callback that only logged its fault would keep the webserver from
        starting. That is the very thing this handler is here to prevent.
        """
        sm = _FakeSecurityManager()

        def expire(role, permission):
            role.permissions = _ExpiredPermissions()

        sm.add_permission_to_role = expire

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        assert sm.get_session.rollbacks == 1
        assert any(
            "Op" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_a_rollback_that_raises_in_turn_is_still_only_a_warning(self, caplog):
        """A database gone for good refuses the rollback as well as the statement.

        Letting that one out of the handler would stop the webserver over the very
        clean-up meant to keep it running.
        """
        sm = _FakeSecurityManager(session_raises=RuntimeError("no database"))
        sm.find_role = lambda name: (_ for _ in ()).throw(RuntimeError("no database"))

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.plugin"
        ), patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        messages = [r.getMessage() for r in caplog.records]
        assert sm.get_session.rollbacks == 1
        assert any("rolled back" in m for m in messages), messages


@contextlib.contextmanager
def _option(value: str | None):
    """Run the block with the switch set to ``value`` in the process environment.

    ``conf`` reads an environment variable before it reads ``airflow.cfg``, so this is
    the same channel an operator uses who configures Airflow through its environment.
    ``None`` runs the block with the option set nowhere.
    """
    name = f"AIRFLOW__{GRANT_OP_ACCESS_SECTION.upper()}__{GRANT_OP_ACCESS_OPTION.upper()}"
    with patch.dict(os.environ, {}, clear=False):
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
        yield


def _read_switch(default: bool) -> bool:
    return read_flag(GRANT_OP_ACCESS_SECTION, GRANT_OP_ACCESS_OPTION, default)


class _AlwaysTrueBackend:
    """A secrets backend answering every Variable with a value that means yes."""

    def get_variable(self, key: str) -> str:
        return "true"


class TestReadFlag:
    """The yes-or-no reader: every spelling, the default, and a value it cannot read.

    Its answer decides whether a role keeps a permission it can start DAG runs with, so
    the one thing it must never do is read something that is not an answer as a yes.
    """

    @pytest.mark.parametrize(
        "raw", ["1", "t", "true", "TRUE", " True ", "y", "yes", "on", "ON", "Yes # open"]
    )
    def test_a_value_spelled_as_yes_reads_as_true(self, raw):
        with _option(raw):
            assert _read_switch(False) is True

    @pytest.mark.parametrize(
        "raw",
        ["0", "f", "false", "FALSE", " False ", "n", "no", "off", "OFF", "No # closed"],
    )
    def test_a_value_spelled_as_no_reads_as_false(self, raw):
        """Every spelling of no an ini file is written in closes the page.

        ``off`` and ``no`` are what an administrator reaches for first, and a reader
        that knew only ``false`` would answer the switch he wrote to close the page by
        leaving it open.
        """
        with _option(raw):
            assert _read_switch(True) is False

    def test_an_unset_option_reads_as_the_default(self):
        with _option(None):
            assert _read_switch(True) is True
            assert _read_switch(False) is False

    @pytest.mark.parametrize("raw", ["", "   ", "maybe", "2", "ture", "# no"])
    def test_a_value_in_no_known_spelling_reads_as_false(self, raw, caplog):
        """A switch holding something unreadable is not an instruction to grant.

        The switch has one job, and it is to take a permission away; the granting answer
        is the one that needs a value saying so. An administrator who wrote a typo over
        the default was reaching for the closed page, and the value he wrote is named in
        the log so that he can find it.
        """
        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.tunables"
        ), _option(raw):
            assert _read_switch(True) is False

        messages = [r.getMessage() for r in caplog.records]
        assert any(GRANT_OP_ACCESS_OPTION in m and repr(raw) in m for m in messages), (
            messages
        )

    def test_a_configuration_that_cannot_answer_reads_as_false(self, caplog):
        """A raw string the parser refuses to build is as unreadable as a typo.

        A value like ``%(missing)s`` in ``airflow.cfg`` fails interpolation, and the
        answer is the same one every unreadable value gets: the privilege is withheld.
        """
        import configparser

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.tunables"
        ), _option("false"), patch(
            "airflow.configuration.conf.get",
            side_effect=configparser.InterpolationMissingOptionError(
                GRANT_OP_ACCESS_OPTION, GRANT_OP_ACCESS_SECTION, "%(missing)s", "missing"
            ),
        ):
            assert _read_switch(True) is False

        assert any(
            GRANT_OP_ACCESS_OPTION in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_the_option_is_read_from_the_configuration_alone(self):
        """The switch answers to the configuration and to nothing else.

        A read reaching the metadata database — directly, or through the secrets
        backends Airflow puts in front of it — is a read that can hang while the
        webserver is building its application, and one that a role holding write access
        to Variables can answer in the administrator's place.
        """
        def refuse(*args, **kwargs):
            raise AssertionError("the switch was read outside the configuration")

        with _option("false"), patch(
            "airflow.configuration.ensure_secrets_loaded", side_effect=refuse
        ), patch("airflow.models.Variable.get", side_effect=refuse):
            assert _read_switch(True) is False


class TestTheSwitchClosesThePageInEverySpellingOfNo:
    """What an administrator writes to close the page closes it, however he spells it.

    The callback runs here over the real reader and the real configuration, because the
    property at stake is the one an administrator sees: the option in his ``airflow.cfg``
    and what the Op role holds after the restart he made for it.
    """

    @pytest.mark.parametrize("raw", ["off", "OFF", "no", "n", "false", "0", "ture"])
    def test_the_role_ends_without_the_page(self, raw):
        sm = _FakeSecurityManager(permissions=_OP_PERMISSIONS)

        with _option(raw):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set()
        assert set(sm.removed) == set(_OP_PERMISSIONS)

    def test_a_page_opened_by_the_default_is_closed_by_the_next_start(self):
        """The timeline an installation actually goes through.

        The provider ships with the page open, so the first webserver start hands the
        Op role all six permissions and the metadata database keeps them. The
        administrator then writes the switch and restarts, and that restart is the only
        thing standing between the role and the right to start a DAG run from a queue
        message — it has to take all six back.
        """
        sm = _FakeSecurityManager()

        with _option(None):
            _grant_op_access(_state(sm))
        assert sm.roles["Op"].permissions == set(_OP_PERMISSIONS)

        with _option("off"):
            _grant_op_access(_state(sm))
        assert sm.roles["Op"].permissions == set()


class TestTheSwitchIsBeyondTheRoleItGoverns:
    """The switch withdraws a privilege from Op, so Op must not be able to set it.

    Airflow hands the Op role full write access to Variables and read-only access to the
    configuration, and that is what decides where a switch over that role's permissions
    can live: one kept in a Variable is one the role turns back on for itself at the
    next webserver start.
    """

    @staticmethod
    def _op_permissions():
        override = pytest.importorskip(
            "airflow.providers.fab.auth_manager.security_manager.override"
        )
        for config in override.FabAirflowSecurityManagerOverride.ROLE_CONFIGS:
            if config["role"] == "Op":
                return set(config["perms"])
        raise AssertionError("the FAB security manager declares no Op role")

    def test_the_op_role_writes_variables(self):
        from airflow.security import permissions

        held = self._op_permissions()
        for action in (
            permissions.ACTION_CAN_CREATE,
            permissions.ACTION_CAN_EDIT,
            permissions.ACTION_CAN_DELETE,
        ):
            assert (action, permissions.RESOURCE_VARIABLE) in held

    def test_the_op_role_does_not_write_the_configuration(self):
        from airflow.security import permissions

        held = self._op_permissions()
        for action in (
            permissions.ACTION_CAN_CREATE,
            permissions.ACTION_CAN_EDIT,
            permissions.ACTION_CAN_DELETE,
        ):
            assert (action, permissions.RESOURCE_CONFIG) not in held

    def test_the_switch_is_named_as_a_configuration_option(self):
        """What the plugin reads is a section and an option, and no Variable name."""
        assert (GRANT_OP_ACCESS_SECTION, GRANT_OP_ACCESS_OPTION) == (
            "rmq_watcher", "grant_op_access",
        )
        assert not hasattr(tunables, "GRANT_OP_ACCESS_VAR")

    def test_a_metastore_value_does_not_reopen_the_page(self):
        """The switch says no while everything the Op role can write says yes.

        This is what the option buys: a role the page was taken from writes an Airflow
        Variable of whatever name it likes and gets nowhere, because the grant asks the
        configuration and never the metadata database.
        """
        sm = _FakeSecurityManager(permissions=_OP_PERMISSIONS)

        with _option("false"), patch(
            "airflow.configuration.ensure_secrets_loaded",
            return_value=[_AlwaysTrueBackend()],
        ), patch("airflow.models.Variable.get", return_value="true"):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set()
        assert set(sm.removed) == set(_OP_PERMISSIONS)


class TestGrantOpAccessIsDeclared:
    """The switch is declared in the provider's configuration metadata.

    A declared option is one Airflow's Configuration page, ``airflow config list`` and
    ``airflow config get-value`` show together with its default, so an operator can see
    the option that governs who reaches the page without reading the source.
    """

    @staticmethod
    def _declared_option():
        from airflow_provider_rmq import get_provider_info

        config = get_provider_info()["config"]
        assert GRANT_OP_ACCESS_SECTION in config, (
            f"section [{GRANT_OP_ACCESS_SECTION}] is not described"
        )
        section = config[GRANT_OP_ACCESS_SECTION]
        assert GRANT_OP_ACCESS_OPTION in section["options"], (
            f"option {GRANT_OP_ACCESS_OPTION} is not described"
        )
        return section["options"][GRANT_OP_ACCESS_OPTION]

    def test_the_option_is_described_as_a_boolean(self):
        option = self._declared_option()
        assert option["type"] == "boolean"
        assert option["description"]

    def test_the_declaration_validates_against_the_provider_info_schema(self):
        """A declaration Airflow rejects makes it reject the whole provider.

        The package would then install and import while its hooks, operators, plugin
        and connection type were all absent from the running Airflow, which is a far
        larger failure than the invisible option the declaration exists to fix.
        """
        import json
        import os

        import airflow
        import jsonschema

        from airflow_provider_rmq import get_provider_info

        schema_path = os.path.join(
            os.path.dirname(airflow.__file__), "provider_info.schema.json"
        )
        with open(schema_path) as fh:
            schema = json.load(fh)

        jsonschema.validate(instance=get_provider_info(), schema=schema)

    def test_the_declared_default_is_the_fallback_the_plugin_reads_with(self):
        """The two answers to "unset means what?" are held in different files.

        The declaration is what an operator is shown; the fallback is what the grant
        actually acts on. Left to drift apart they would say opposite things about a
        page nobody configured, and the shown one would be the lie.
        """
        spellings = {"1": True, "t": True, "true": True,
                     "0": False, "f": False, "false": False}
        declared = self._declared_option()["default"]
        assert declared.strip().lower() in spellings, (
            f"declared default {declared!r} is in no spelling Airflow reads as a boolean"
        )

        sm = _FakeSecurityManager()
        app = flask.Flask(__name__)
        app.appbuilder = SimpleNamespace(sm=sm)
        fallbacks: list = []

        def record(section, option, default):
            fallbacks.append(default)
            return True

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", record):
            app.register_blueprint(_bp)

        assert fallbacks == [spellings[declared.strip().lower()]]
