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
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    options = ClientOptions(httpx_client=_build_httpx_client())
    return create_client(url, key, options=options)


async def get_async_supabase_client() -> AsyncClient:
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    options = AsyncClientOptions(httpx_client=_build_async_httpx_client())
    return await create_async_client(url, key, options=options)


def execute_with_retry(build_query: Callable[[], T]) -> T:
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