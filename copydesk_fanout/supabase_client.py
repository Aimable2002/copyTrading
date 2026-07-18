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
"""

from __future__ import annotations

import os
import dotenv
dotenv.load_dotenv()   
from supabase import AsyncClient, Client, create_async_client, create_client

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
    return create_client(url, key)


async def get_async_supabase_client() -> AsyncClient:
    """Async client - required for Realtime subscriptions (ConfigStore's
    config-sync listener). Only ever call this from inside the asyncio
    loop that owns the realtime connection."""
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    return await create_async_client(url, key)