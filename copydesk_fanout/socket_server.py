from __future__ import annotations

import logging
import os
from typing import Any

import jwt
import socketio

logger = logging.getLogger("socket_server")

_ALLOWED_ORIGINS = "*"

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=_ALLOWED_ORIGINS,
)
asgi_app = socketio.ASGIApp(sio)

_session_users: dict[str, str] = {}


def verify_supabase_jwt(token: str) -> str:
    header = jwt.get_unverified_header(token)
    algorithm = header.get("alg", "HS256")

    if algorithm == "HS256":
        secret = os.environ.get("SUPABASE_JWT_SECRET")
        if not secret:
            raise RuntimeError(
                "SUPABASE_JWT_SECRET is not set - cannot verify this HS256 token. "
                "Copy .env.example and fill it in before running the socket server."
            )
        payload = jwt.decode(token, secret, algorithms=["HS256"], audience="authenticated", leeway=1200)
    else:
        signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
        payload = jwt.decode(token, signing_key.key, algorithms=[algorithm], audience="authenticated", leeway=1200)

    return payload["sub"]


_jwks_client: "jwt.PyJWKClient | None" = None


def _get_jwks_client() -> "jwt.PyJWKClient":
    global _jwks_client
    if _jwks_client is None:
        supabase_url = os.environ.get("SUPABASE_URL")
        if not supabase_url:
            raise RuntimeError(
                "SUPABASE_URL is not set - needed to fetch the JWKS endpoint for verifying "
                "asymmetric (ES256/RS256) tokens."
            )
        jwks_url = f"{supabase_url.rstrip('/')}/auth/v1/.well-known/jwks.json"
        _jwks_client = jwt.PyJWKClient(jwks_url, cache_keys=True)
    return _jwks_client


@sio.event
async def connect(sid: str, environ: dict[str, Any], auth: dict[str, Any] | None) -> bool:
    token = (auth or {}).get("token")
    if not token:
        query_string = environ.get("QUERY_STRING", "")
        params = dict(pair.split("=", 1) for pair in query_string.split("&") if "=" in pair)
        token = params.get("token")

    if not token:
        logger.warning("Connection %s rejected: no auth token provided", sid)
        return False

    try:
        user_id = verify_supabase_jwt(token)
    except jwt.InvalidTokenError:
        logger.warning("Connection %s rejected: invalid/expired token", sid)
        return False

    _session_users[sid] = user_id
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
    import uvicorn

    resolved_port = port or int(os.environ.get("PORT", "8000"))
    logger.info("Starting Socket.IO server on 0.0.0.0:%d (point ngrok at this port)", resolved_port)
    uvicorn.run(asgi_app, host="0.0.0.0", port=resolved_port, log_level="info")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run_server()