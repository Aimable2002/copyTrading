"""
Socket.IO server - the direct backend<->frontend channel for live MT
account data (balance/open positions), per the "frontend talks to Supabase
for config/payments/monitoring setup, but reads live account data straight
from the backend" decision.

One-way by design: server -> frontend only. Nothing here accepts trade
actions or config changes from a client; that boundary was decided
explicitly (manual trade intervention happens in the user's own MT5
terminal, config/payments go through Supabase) and this module doesn't
reopen it.

Run behind ngrok:
    - This binds to 0.0.0.0:$PORT (see run_server()) - ngrok can only
      tunnel a port that's actually listening on all interfaces, not just
      localhost.
    - Whatever ngrok assigns as the public https URL for this run needs to
      be in ALLOWED_ORIGINS (see .env.example) so the browser's Socket.IO
      client is allowed to connect - Socket.IO enforces CORS same as any
      other browser-facing server. ngrok's free tier URL changes every
      restart, so ALLOWED_ORIGINS is read fresh from the environment each
      run rather than hardcoded.
    - See ngrok.yml alongside this file for the tunnel definition, and
      README_NGROK.md for the two-terminal (uvicorn + ngrok) run sequence.

Auth: every client must connect with a Supabase auth JWT (the same access
token the frontend already holds from Supabase Auth) as a query param or
in the `auth` payload. This server verifies it against Supabase's JWT
secret and only then joins the client to that user's account rooms - a
client can never subscribe to another user's account data.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import jwt
import socketio

logger = logging.getLogger("socket_server")

# Read fresh at import time from the environment, not hardcoded, since
# ngrok's URL changes across runs unless you're on a paid static domain.
# Comma-separated in .env, e.g. "https://abcd1234.ngrok-free.app,http://localhost:5173"
_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("ALLOWED_ORIGINS", "").split(",")
    if origin.strip()
] or "*"  # falls back to allow-all only if the env var is genuinely unset - fine for a first local run, tighten before real signups

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=_ALLOWED_ORIGINS,
)
asgi_app = socketio.ASGIApp(sio)

# sid -> user_id, so disconnect/logging can reference who a session belonged to
_session_users: dict[str, str] = {}


def _verify_supabase_jwt(token: str) -> str:
    """Returns the user_id (the JWT's `sub` claim) if the token is a valid,
    unexpired Supabase-issued access token. Raises jwt.InvalidTokenError
    (or a subclass) otherwise - callers must catch this and reject the
    connection, never let a client through unauthenticated.

    SUPABASE_JWT_SECRET is Project Settings -> API -> JWT Settings -> JWT
    Secret in the Supabase dashboard - NOT the anon key and NOT the service
    role key. It's what Supabase itself signs user access tokens with.
    """
    secret = os.environ.get("SUPABASE_JWT_SECRET")
    if not secret:
        raise RuntimeError(
            "SUPABASE_JWT_SECRET is not set - cannot verify any client connection. "
            "Copy .env.example and fill it in before running the socket server."
        )
    payload = jwt.decode(token, secret, algorithms=["HS256"], audience="authenticated")
    return payload["sub"]


@sio.event
async def connect(sid: str, environ: dict[str, Any], auth: dict[str, Any] | None) -> bool:
    token = (auth or {}).get("token")
    if not token:
        # Also accept it as a query param (?token=...) - some Socket.IO
        # client setups find that easier to attach than the `auth` payload.
        query_string = environ.get("QUERY_STRING", "")
        params = dict(pair.split("=", 1) for pair in query_string.split("&") if "=" in pair)
        token = params.get("token")

    if not token:
        logger.warning("Connection %s rejected: no auth token provided", sid)
        return False

    try:
        user_id = _verify_supabase_jwt(token)
    except jwt.InvalidTokenError:
        logger.warning("Connection %s rejected: invalid/expired token", sid)
        return False

    _session_users[sid] = user_id
    # Room per user, not per account - a user with multiple accounts (one
    # master + several followers) gets all of their own live-state emits
    # through this single room join, no per-account subscribe call needed
    # from the frontend.
    await sio.enter_room(sid, f"user:{user_id}")
    logger.info("Connection %s authenticated as user %s", sid, user_id)
    return True


@sio.event
async def disconnect(sid: str) -> None:
    user_id = _session_users.pop(sid, None)
    logger.info("Disconnected %s (user %s)", sid, user_id)


# ------------------------------------------------------------------ #
# Emit helpers - called from the live-state publisher loop (see
# live_state_publisher.py), never from a client handler. Receive-only
# boundary is enforced simply by not registering any other @sio.event
# handlers above - there is nothing for a client to call.
# ------------------------------------------------------------------ #
async def emit_account_state(user_id: str, account_id: str, state: dict[str, Any]) -> None:
    await sio.emit("account_state", {"account_id": account_id, **state}, room=f"user:{user_id}")


def run_server(port: int | None = None) -> None:
    """Starts the ASGI app with uvicorn, bound to 0.0.0.0 so ngrok can
    reach it. Call via `python -m copydesk_fanout.socket_server` for a
    standalone run, or import run_server() from main.py to run it
    alongside the fanout agents in one process."""
    import uvicorn

    resolved_port = port or int(os.environ.get("PORT", "8000"))
    logger.info("Starting Socket.IO server on 0.0.0.0:%d (point ngrok at this port)", resolved_port)
    uvicorn.run(asgi_app, host="0.0.0.0", port=resolved_port, log_level="info")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_server()