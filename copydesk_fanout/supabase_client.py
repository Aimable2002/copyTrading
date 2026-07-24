"""
Central place to build Supabase clients for the backend.

The backend always uses the SERVICE ROLE key, never the anon key - it
needs to read/write order_pairs and pending_copies, which have RLS
enabled with zero policies (i.e. only the service role can touch them at
all, per supabase/schema.sql). The frontend uses its own anon/authenticated
key directly and never goes through this module.

Two clients are exposed because Realtime (used for config sync in
ConfigStore) is only available on the async client in supabase-py; every
other read/write in this codebase (OrderPairStore, live-state publisher)
is synchronous and runs from plain threads, so it uses the sync client.

Both clients are also given their own explicitly-configured httpx
transport (see _build_httpx_client/_build_async_httpx_client below) and
this module exposes execute_with_retry/async_execute_with_retry helpers,
because the raw supabase-py client is a single long-lived connection pool
reused for the entire process lifetime. On an unstable network (or just an
idle connection getting closed by Supabase's edge/proxy between requests),
the *next* request to try reusing that connection fails with
httpx.RemoteProtocolError ("Server disconnected") - not because the query
was wrong, but because the socket it was about to be sent on no longer
exists. That's the exact crash seen on /masters/directory and
/masters/{id}/trades: the very next identical request succeeds, because
httpcore evicts the dead connection and opens a fresh one - so a single
retry is normally enough to fix it. Every call site that talks to Postgrest
should go through one of these retry helpers rather than calling
`.execute()` directly.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Awaitable, Callable, TypeVar

import dotenv
dotenv.load_dotenv()

import httpx
from supabase import AsyncClient, Client, ClientOptions, create_async_client, create_client
from supabase.lib.client_options import AsyncClientOptions

logger = logging.getLogger("supabase_client")

T = TypeVar("T")

# httpcore's own `retries=` only covers failures during the initial TCP
# connect (ConnectError/ConnectTimeout) - it does NOT cover a pooled
# keep-alive connection that gets killed after it was already established,
# which is what RemoteProtocolError actually is here. That's handled by the
# application-level retry helpers below instead. keepalive_expiry is still
# turned down from httpx's 5s default so more idle connections get
# proactively recycled client-side before something else silently kills
# them - a mitigation, not a fix, hence the retry helpers still exist.
_CONNECT_TIMEOUT_SECONDS = 10.0
_KEEPALIVE_EXPIRY_SECONDS = 4.0
_LIMITS = httpx.Limits(max_keepalive_connections=20, max_connections=100, keepalive_expiry=_KEEPALIVE_EXPIRY_SECONDS)

_RETRYABLE_EXCEPTIONS = (
    httpx.RemoteProtocolError,
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.WriteError,
    httpx.PoolTimeout,
)

_MAX_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 0.5


def _build_httpx_client() -> httpx.Client:
    return httpx.Client(
        http2=True,
        follow_redirects=True,
        limits=_LIMITS,
        timeout=httpx.Timeout(120.0, connect=_CONNECT_TIMEOUT_SECONDS),
        transport=httpx.HTTPTransport(retries=1, http2=True),
    )


def _build_async_httpx_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        http2=True,
        follow_redirects=True,
        limits=_LIMITS,
        timeout=httpx.Timeout(120.0, connect=_CONNECT_TIMEOUT_SECONDS),
        transport=httpx.AsyncHTTPTransport(retries=1, http2=True),
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable {name}. Copy .env.example to "
            f".env and fill in your Supabase project URL and service role key."
        )
    return value


def get_supabase_client() -> Client:
    """Sync client - used for OrderPairStore write-through/rebuild and the
    live-state publisher. Safe to call from worker threads."""
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    options = ClientOptions(httpx_client=_build_httpx_client())
    return create_client(url, key, options=options)


async def get_async_supabase_client() -> AsyncClient:
    """Async client - required for Realtime subscriptions (ConfigStore's
    config-sync listener). Only ever call this from inside the asyncio
    loop that owns the realtime connection."""
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    options = AsyncClientOptions(httpx_client=_build_async_httpx_client())
    return await create_async_client(url, key, options=options)


def execute_with_retry(build_query: Callable[[], T]) -> T:
    """Runs a sync Postgrest query with retries on transient connection
    failures. `build_query` is a zero-arg callable that builds the query
    AND calls `.execute()` on it, e.g.:

        execute_with_retry(
            lambda: supabase_client.table("t").select("*").execute()
        )

    A callable (rather than an already-built query object) so each retry
    attempt constructs its request fresh rather than trying to resend
    something that may have failed partway through.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return build_query()
        except _RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
            if attempt == _MAX_ATTEMPTS:
                break
            logger.warning(
                "Transient Supabase connection error (attempt %d/%d): %s - retrying",
                attempt, _MAX_ATTEMPTS, exc,
            )
            time.sleep(_RETRY_BACKOFF_SECONDS * attempt)
    assert last_exc is not None
    raise last_exc


async def async_execute_with_retry(build_query: Callable[[], Awaitable[T]]) -> T:
    """Async counterpart of execute_with_retry, for the Realtime config-sync
    client's own Postgrest queries (initial load + on_change refetch)."""
    import asyncio

    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return await build_query()
        except _RETRYABLE_EXCEPTIONS as exc:
            last_exc = exc
            if attempt == _MAX_ATTEMPTS:
                break
            logger.warning(
                "Transient Supabase connection error (attempt %d/%d): %s - retrying",
                attempt, _MAX_ATTEMPTS, exc,
            )
            await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)
    assert last_exc is not None
    raise last_exc