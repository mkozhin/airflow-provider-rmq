from __future__ import annotations

import json
import ssl
from typing import Any


def build_ssl_context(extras: dict[str, Any]) -> ssl.SSLContext | None:
    """Build an :class:`ssl.SSLContext` from Airflow connection extras.

    Supports two layouts of SSL settings in ``extras``:

    1. **Nested** (manual JSON in the *Extra* field)::

           {"ssl_enabled": true, "ssl_options": {"ca_certs": "/path/ca.pem", ...}}

    2. **Flat** (populated by connection form widgets)::

           {"ssl_enabled": true, "ca_certs": "/path/ca.pem", ...}

    When both layouts are present, nested ``ssl_options`` values take precedence.

    :param extras: The ``extra_dejson`` dict from an Airflow connection.
    :type extras: dict[str, Any]
    :return: Configured SSL context, or ``None`` if SSL is not enabled.
    :rtype: ssl.SSLContext | None
    """
    if not extras.get("ssl_enabled", False):
        return None

    ssl_context = ssl.create_default_context()

    # Merge flat extras with nested ssl_options (nested takes precedence)
    ssl_opts_raw = extras.get("ssl_options", {})
    if isinstance(ssl_opts_raw, str):
        try:
            ssl_opts_raw = json.loads(ssl_opts_raw)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in ssl_options: {e}") from e

    def _get(key: str) -> Any:
        """Look up key in nested ssl_options first, then flat extras."""
        return ssl_opts_raw.get(key) or extras.get(key)

    ca_certs = _get("ca_certs")
    certfile = _get("certfile")
    keyfile = _get("keyfile")
    cert_reqs = _get("cert_reqs")

    if ca_certs:
        ssl_context.load_verify_locations(ca_certs)
    if certfile and keyfile:
        ssl_context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    if cert_reqs == "CERT_NONE":
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    return ssl_context