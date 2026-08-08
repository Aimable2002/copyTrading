"""
cTrader's OAuth implementation does NOT round-trip a `state` query param back
to the redirect URI - confirmed against a live run: the callback only ever
arrives with `?code=...`, nothing else, matching Spotware's own docs ("they
are redirected to your specified redirect URI with the authorization code
appended as a query parameter") and their SDK's getAuthUri() helper, which
only supports `scope`/`baseUri`, no `state`.

That rules out the usual "sign user_id into state, verify it on the way back"
pattern - there's nothing for the callback to read except `code`. This module
is the replacement: a short-lived server-side record of "who just clicked
Connect", set at /accounts/ctrader/start and consumed at
/accounts/ctrader/callback.

KNOWN LIMITATION: this is a single-flight store - only one cTrader connect
attempt can be in progress across the whole process at a time. That's fine for
now (you're the one provisioning master accounts, not end users self-serving
concurrently), but if this ever opens up to multiple people connecting
accounts at the same time, a second concurrent /start call will silently steal
the pending slot from the first. Revisit with a proper per-attempt token
(passed via a first-party cookie, or a pending_consents table keyed by a
random ID embedded in the redirect_uri path if cTrader's exact-match
requirement tolerates that) before this becomes multi-user.
"""

from __future__ import annotations

import threading
import time

_TTL_SECONDS = 600  # matches how long a user has to complete Spotware's consent page

_lock = threading.Lock()
_pending: dict[str, float] | None = None  # {"user_id": ..., "created_at": ...} or None


def set_pending_user(user_id: str) -> None:
    global _pending
    with _lock:
        _pending = {"user_id": user_id, "created_at": time.time()}


def consume_pending_user() -> str | None:
    """Returns the user_id and clears the slot, or None if nothing's pending / it expired."""
    global _pending
    with _lock:
        if _pending is None:
            return None
        if time.time() - _pending["created_at"] > _TTL_SECONDS:
            _pending = None
            return None
        user_id = _pending["user_id"]
        _pending = None
        return user_id