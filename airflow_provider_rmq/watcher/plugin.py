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


#: The action-resource pairs that make the Subscriptions page usable. Read access alone
#: would draw a page whose every button answers with a refusal: the template renders its
#: controls for whoever got the page at all.
_OP_PERMISSIONS = (
    ("can_read", RMQWatcherView.class_permission_name),
    ("can_create", RMQWatcherView.class_permission_name),
    ("can_edit", RMQWatcherView.class_permission_name),
    ("can_delete", RMQWatcherView.class_permission_name),
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
    leave the role holding full access after the administrator declined it. The switch
    is read first and on its own: an answer that cannot be had leaves the role exactly
    as it is, so a database that refuses the read never re-grants what an administrator
    took away.

    Runs as a deferred blueprint callback, by which point ``app.appbuilder`` exists and
    an application context is up. Nothing it does may keep the webserver from starting,
    so every failure ends as a warning over a session handed back rolled back.
    """
    grant: bool | None = None
    try:
        # The switch is read before anything is touched, and an unreadable one raises:
        # what the role holds then stays exactly as it is, and ``grant`` still being
        # None is what tells the two failures apart down below.
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
        done = 0
        for action, resource in _OP_PERMISSIONS:
            if grant:
                # The permission is created rather than looked up: with
                # ``[fab] update_fab_perms`` off nobody else creates it by the time this
                # runs, and FAB hands back the existing one when there is one. It
                # answers None when the database refused the insert.
                permission = sm.create_permission(action, resource)
                if permission is None:
                    log.warning(
                        "RMQ Watcher: permission %s on %r could not be created — role "
                        "%s does not get it",
                        action, resource, _OP_ROLE,
                    )
                    continue
                sm.add_permission_to_role(role, permission)
                landed = permission in role.permissions
            else:
                # A pair FAB does not hold is a pair no role holds either, so the
                # revocation asks for the existing one instead of making it first.
                permission = sm.get_permission(action, resource)
                if permission is None:
                    # A pair FAB never made is one the role cannot be holding, so it is
                    # already off the role and counts towards the summary below.
                    done += 1
                    continue
                sm.remove_permission_from_role(role, permission)
                landed = permission not in role.permissions
            # Both FAB calls answer nothing and raise nothing: a commit they could not
            # make is logged by FAB, rolled back and returned from as if it had worked.
            # What the role holds afterwards is therefore the only account of the write
            # this callback can give, and the summary below counts nothing else.
            if not landed:
                log.warning(
                    "RMQ Watcher: permission %s on %r was not %s role %s — the write "
                    "did not reach the database",
                    action, resource, "given to" if grant else "taken from", _OP_ROLE,
                )
                continue
            done += 1
        log.info(
            "RMQ Watcher: role %s %s %d of the %d permissions of the Subscriptions page",
            _OP_ROLE,
            "holds" if grant else "is without",
            done,
            len(_OP_PERMISSIONS),
        )
    except Exception:
        # Every statement above runs on FAB's session, and Airflow reuses that same
        # session right after this callback to synchronise its own roles. A statement
        # that failed leaves the transaction aborted, and the synchronisation then
        # raises on a database that has already recovered, taking the webserver start
        # down with it — the outcome this handler exists to prevent. The rollback hands
        # it a clean transaction, and can fail in turn, so it is guarded on its own.
        try:
            state.app.appbuilder.sm.get_session.rollback()
        except Exception:
            log.warning(
                "RMQ Watcher: the session could not be rolled back after the "
                "Subscriptions page access of role %s failed",
                _OP_ROLE,
                exc_info=True,
            )
        if grant is None:
            log.warning(
                "RMQ Watcher: Variable %s could not be read — the Subscriptions page "
                "permissions of role %s are left as they are",
                GRANT_OP_ACCESS_VAR,
                _OP_ROLE,
                exc_info=True,
            )
        else:
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
