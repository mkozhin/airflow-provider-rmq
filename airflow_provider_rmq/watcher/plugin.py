from __future__ import annotations

import logging
import os

from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

from airflow_provider_rmq.watcher.listener import RMQWatcherListener
from airflow_provider_rmq.watcher.models import ensure_table_exists
from airflow_provider_rmq.watcher.tunables import GRANT_OP_ACCESS_VAR, read_flag
from airflow_provider_rmq.watcher.views import RMQWatcherView

#: The menu entry and the category it hangs under. FAB grants menu access by the
#: displayed names, so the registration below and the grant further down read the same
#: two strings.
_MENU_NAME = "Subscriptions"
_MENU_CATEGORY = "RabbitMQ"

#: The role the provider opens the page for. Admin gets it from Airflow itself, which
#: hands that role every non-DAG permission on every role synchronisation.
_OP_ROLE = "Op"

_bp = Blueprint(
    "rmq_watcher",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)


def op_permissions() -> tuple[tuple[str, str], ...]:
    """The action-resource pairs that make the Subscriptions page usable.

    Read access alone would draw a page whose every button answers with a refusal: the
    template renders its controls for whoever got the page at all.
    """
    resource = RMQWatcherView.class_permission_name
    return (
        ("can_read", resource),
        ("can_create", resource),
        ("can_edit", resource),
        ("can_delete", resource),
        ("menu_access", _MENU_NAME),
        ("menu_access", _MENU_CATEGORY),
    )


def _grant_op_access(state) -> None:
    """Give the Op role every permission of the Subscriptions page, or take them back.

    The fixed role lists of Airflow's ``sync_roles`` hold no resource belonging to a
    third-party plugin, so the page otherwise belongs to Admin alone. The Op role
    configures the instance the subscriptions listen on, and this hands it the page at
    every webserver start, which makes the grant retroactive over an upgrade.

    Airflow Variable :data:`GRANT_OP_ACCESS_VAR` governs the access, not merely the
    grant: a false value **removes** the same permissions, because FAB deletes no valid
    role-permission pair of its own accord and a switch that only fell silent would
    leave the role holding full access after the administrator declined it.

    Runs as a deferred blueprint callback, by which point ``app.appbuilder`` exists and
    an application context is up. Nothing it does may keep the webserver from starting,
    so every failure ends as a warning.
    """
    try:
        grant = read_flag(GRANT_OP_ACCESS_VAR, True)
        sm = state.app.appbuilder.sm
        role = sm.find_role(_OP_ROLE)
        if role is None:
            log.warning(
                "RMQ Watcher: role %s does not exist yet — the Subscriptions page will "
                "be opened to it at the next webserver start",
                _OP_ROLE,
            )
            return
        for action, resource in op_permissions():
            # The permissions are created rather than looked up: with
            # ``[fab] update_fab_perms`` off nobody else creates them by the time this
            # runs, and FAB hands back the existing one when there is one.
            permission = sm.create_permission(action, resource)
            if permission is None:
                continue
            if grant:
                sm.add_permission_to_role(role, permission)
            else:
                sm.remove_permission_from_role(role, permission)
        log.info(
            "RMQ Watcher: %s role %s the Subscriptions page",
            "granted" if grant else "revoked from",
            _OP_ROLE,
        )
    except Exception:
        log.warning(
            "RMQ Watcher: failed to set the Subscriptions page access of role %s",
            _OP_ROLE,
            exc_info=True,
        )


_bp.record_once(_grant_op_access)


class RMQWatcherPlugin(AirflowPlugin):
    name = "rmq_watcher"
    listeners = [RMQWatcherListener()]
    flask_blueprints = [_bp]
    appbuilder_views = [
        {
            "name": _MENU_NAME,
            "category": _MENU_CATEGORY,
            "view": RMQWatcherView(),
        }
    ]

    @classmethod
    def on_load(cls, *args, **kwargs):
        """Create rmq_watcher_* tables when the plugin is first loaded."""
        try:
            ensure_table_exists()
        except Exception:
            log.exception("RMQ Watcher: failed to create DB tables on plugin load")
