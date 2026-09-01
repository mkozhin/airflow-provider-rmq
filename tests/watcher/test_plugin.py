"""Tests for the plugin module: what it registers with Airflow, the Subscriptions
page access it gives the Op role at webserver start, and the switch that governs it.
"""
from __future__ import annotations

import contextlib
import logging
from types import SimpleNamespace
from unittest.mock import patch

import flask
import pytest

from airflow_provider_rmq.watcher.plugin import (
    _OP_PERMISSIONS,
    RMQWatcherPlugin,
    _bp,
    _grant_op_access,
)
from airflow_provider_rmq.watcher.tunables import GRANT_OP_ACCESS_VAR, read_flag


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
        arguments of the read are recorded here too: they name the Variable an operator
        is told to set, and no other test reads that name off the call.
        """
        sm = _FakeSecurityManager()
        app = flask.Flask(__name__)
        app.appbuilder = SimpleNamespace(sm=sm)
        reads: list = []

        def record(name, default):
            reads.append((name, default))
            return True

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", record):
            app.register_blueprint(_bp)

        assert reads == [("rmq_watcher_grant_op_access", True)]
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

    def test_a_switch_that_cannot_be_read_leaves_the_role_as_it_is(self, caplog):
        """An unanswerable switch grants nothing and revokes nothing.

        The read and the permission writes go through different sessions, so one can
        fail while the others work. Reading the failure as the default would hand six
        permissions back to a role an administrator has taken them from, and a
        permission switch that cannot be read has to leave the door where it stands.
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
            GRANT_OP_ACCESS_VAR in r.getMessage() for r in caplog.records
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


class _FakeBackend:
    """A secrets backend holding ``values``, or failing every read with ``raises``."""

    def __init__(self, values: dict, raises: Exception | None = None):
        self._values = values
        self._raises = raises

    def get_variable(self, key: str):
        if self._raises is not None:
            raise self._raises
        return self._values.get(key)


@contextlib.contextmanager
def _backends(*backends):
    with patch("airflow.configuration.ensure_secrets_loaded", return_value=list(backends)):
        yield


class TestReadFlag:
    """The yes-or-no reader: both spellings, the default and the unreadable database.

    Its answer decides whether a role keeps a permission, so the one thing it must never
    do is turn a database that could not answer into an answer.
    """

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", " on "])
    def test_a_value_spelled_as_yes_reads_as_true(self, raw):
        with _backends(_FakeBackend({GRANT_OP_ACCESS_VAR: raw})):
            assert read_flag(GRANT_OP_ACCESS_VAR, False) is True

    @pytest.mark.parametrize("raw", ["0", "false", "FALSE", "No", " off "])
    def test_a_value_spelled_as_no_reads_as_false(self, raw):
        with _backends(_FakeBackend({GRANT_OP_ACCESS_VAR: raw})):
            assert read_flag(GRANT_OP_ACCESS_VAR, True) is False

    def test_an_unset_variable_reads_as_the_default(self):
        with _backends(_FakeBackend({})):
            assert read_flag(GRANT_OP_ACCESS_VAR, True) is True
            assert read_flag(GRANT_OP_ACCESS_VAR, False) is False

    def test_the_backends_are_asked_in_turn(self):
        """Airflow reads a Variable from the first backend that holds it.

        An environment variable overrides the metadata database, and a reader that asked
        the database alone would answer with a value the rest of Airflow ignores.
        """
        with _backends(
            _FakeBackend({}), _FakeBackend({GRANT_OP_ACCESS_VAR: "false"})
        ):
            assert read_flag(GRANT_OP_ACCESS_VAR, True) is False

    @pytest.mark.parametrize("raw", ["", "maybe", "2"])
    def test_a_value_in_no_known_spelling_reads_as_the_default(self, caplog, raw):
        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.tunables"
        ), _backends(_FakeBackend({GRANT_OP_ACCESS_VAR: raw})):
            assert read_flag(GRANT_OP_ACCESS_VAR, True) is True

        assert any(
            GRANT_OP_ACCESS_VAR in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_a_failing_backend_does_not_hide_a_value_the_next_one_holds(self, caplog):
        """Airflow puts a custom backend in front of the metadata database.

        A Vault or SSM outage would otherwise swallow the value the database holds, and
        the switch would read as unreadable at every webserver start for as long as that
        backend is flaky — leaving the role holding a page the administrator set the
        Variable to close.
        """
        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.tunables"
        ), _backends(
            _FakeBackend({}, raises=RuntimeError("vault is down")),
            _FakeBackend({GRANT_OP_ACCESS_VAR: "false"}),
        ):
            assert read_flag(GRANT_OP_ACCESS_VAR, True) is False

        assert any(
            "vault is down" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_a_read_no_backend_answered_raises_the_first_failure(self):
        """A refusal in front of backends that hold nothing is not an unset Variable.

        The failure is what the caller is told about, because it is the one thing that
        stands between it and an answer.
        """
        first = RuntimeError("vault is down")

        with _backends(
            _FakeBackend({}, raises=first), _FakeBackend({})
        ), pytest.raises(RuntimeError) as caught:
            read_flag(GRANT_OP_ACCESS_VAR, True)

        assert caught.value is first

    def test_a_database_that_cannot_answer_raises(self):
        """The caller is told, instead of being handed the default as an answer.

        Airflow's own reader logs a failed backend and returns the ``None`` an unset
        Variable returns, which would make an outage indistinguishable from an
        administrator who set nothing.
        """
        with _backends(
            _FakeBackend({}, raises=RuntimeError("no database"))
        ), pytest.raises(RuntimeError):
            read_flag(GRANT_OP_ACCESS_VAR, False)
