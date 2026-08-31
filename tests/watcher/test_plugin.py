"""Tests for RMQWatcherPlugin registration."""
from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import flask

from airflow_provider_rmq.watcher.plugin import (
    RMQWatcherPlugin,
    _bp,
    _grant_op_access,
    op_permissions,
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


class _FakeSecurityManager:
    """Enough of the FAB security manager for the grant: roles, permissions, both edges.

    ``create_permission`` returns the pair itself, so a role's ``permissions`` set reads
    as the action-resource pairs the callback actually handed it.
    """

    def __init__(self, roles=("Op",), permissions=()):
        self.roles = {name: _FakeRole(name) for name in roles}
        for role in self.roles.values():
            role.permissions.update(permissions)
        self.created: list = []
        self.added: list = []
        self.removed: list = []

    def find_role(self, name):
        return self.roles.get(name)

    def create_permission(self, action, resource):
        self.created.append((action, resource))
        return (action, resource)

    def add_permission_to_role(self, role, permission):
        self.added.append(permission)
        role.permissions.add(permission)

    def remove_permission_from_role(self, role, permission):
        self.removed.append(permission)
        role.permissions.discard(permission)


def _state(sm):
    """A stand-in for the blueprint setup state the callback is handed."""
    return SimpleNamespace(app=SimpleNamespace(appbuilder=SimpleNamespace(sm=sm)))


class TestOpAccessGrant:
    """The provider hands the Subscriptions page to the Op role at webserver start."""

    def test_registering_the_blueprint_runs_the_grant(self):
        """The callback is wired to the blueprint, not merely callable.

        Every other test here calls the function itself and would stay green with the
        registration gone, leaving the grant to never happen on a live webserver.
        """
        sm = _FakeSecurityManager()
        app = flask.Flask(__name__)
        app.appbuilder = SimpleNamespace(sm=sm)

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            app.register_blueprint(_bp)

        assert sm.roles["Op"].permissions == set(op_permissions())

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

        assert set(op_permissions()) == {
            ("can_read", "RMQ Subscriptions"),
            ("can_create", "RMQ Subscriptions"),
            ("can_edit", "RMQ Subscriptions"),
            ("can_delete", "RMQ Subscriptions"),
            ("menu_access", "Subscriptions"),
            ("menu_access", "RabbitMQ"),
        }

    def test_granting_again_over_the_same_role_adds_nothing_new(self):
        sm = _FakeSecurityManager(permissions=op_permissions())

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=True):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set(op_permissions())

    def test_a_false_flag_takes_the_permissions_back(self):
        """The switch governs the access, not just the grant.

        After one start on the default the permissions are in the database, and FAB
        removes a valid role-permission pair in neither ``bulk_sync_roles`` nor
        ``clean_perms``. A role that already holds them is therefore the only state in
        which the revocation can be seen at all.
        """
        sm = _FakeSecurityManager(permissions=op_permissions())

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=False):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set()
        assert set(sm.removed) == set(op_permissions())
        assert not sm.added

    def test_a_false_flag_over_a_role_without_them_is_quiet(self):
        sm = _FakeSecurityManager()

        with patch("airflow_provider_rmq.watcher.plugin.read_flag", return_value=False):
            _grant_op_access(_state(sm))

        assert sm.roles["Op"].permissions == set()
        assert not sm.added

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
