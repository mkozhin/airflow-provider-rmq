"""Shared form-field parsing for ``RMQWatcherView``.

``create()``, ``edit()``, and ``edit_group()`` in ``views.py`` each parse the
same two form fields (``cooldown``, ``filter_data``) with near-identical
inline try/except blocks. This module is the single place where that parsing
lives — pure functions over raw strings, no Flask dependency, so they can be
unit-tested without a request context.
"""

from __future__ import annotations

import json


def parse_cooldown(raw: str) -> int:
    """Parse the ``cooldown`` form field.

    Empty string means "no cooldown" → ``0``. Raises ``ValueError`` if the
    value is not an integer or is negative.
    """
    cooldown = int(raw) if raw else 0
    if cooldown < 0:
        raise ValueError("cooldown must be >= 0")
    return cooldown


def parse_filter_data(raw: str) -> dict:
    """Parse the ``filter_data`` form field.

    Empty string means "no filter" → ``{}``. Raises ``ValueError`` if the
    value is not valid JSON, or if it does not decode to a JSON object.
    """
    if not raw:
        return {}
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("filter_data must be valid JSON") from exc
    if not isinstance(result, dict):
        raise ValueError("filter_data JSON must decode to an object/dict.")
    return result
