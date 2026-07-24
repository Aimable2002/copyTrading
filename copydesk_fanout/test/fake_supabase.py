"""In-memory stand-in for supabase-py's Client, just enough of the query
chain (table/select/insert/update/upsert/eq/in_/lte/order/limit/execute)
for the wallet/billing/roster/master_rate/profit_share modules to run
against in tests, with no real Supabase project needed."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, store: dict, table_name: str):
        self.store = store
        self.table_name = table_name
        self.filters: list[tuple] = []
        self.op = None
        self.payload = None
        self.on_conflict = None
        self.order_col = None
        self.order_desc = False
        self.limit_n = None

    def select(self, *_args, **_kwargs):
        self.op = self.op or "select"
        return self

    def insert(self, payload):
        self.op = "insert"
        self.payload = payload
        return self

    def update(self, payload):
        self.op = "update"
        self.payload = payload
        return self

    def upsert(self, payload, on_conflict=None):
        self.op = "upsert"
        self.payload = payload
        self.on_conflict = on_conflict
        return self

    def eq(self, col, val):
        self.filters.append(("eq", col, val))
        return self

    def in_(self, col, vals):
        self.filters.append(("in", col, list(vals)))
        return self

    def lte(self, col, val):
        self.filters.append(("lte", col, val))
        return self

    def order(self, col, desc=False):
        self.order_col = col
        self.order_desc = desc
        return self

    def limit(self, n):
        self.limit_n = n
        return self

    def _matches(self, row):
        for typ, col, val in self.filters:
            if typ == "eq" and row.get(col) != val:
                return False
            if typ == "in" and row.get(col) not in val:
                return False
            if typ == "lte" and (row.get(col) is None or row.get(col) > val):
                return False
        return True

    def execute(self):
        table = self.store.setdefault(self.table_name, [])
        if self.op == "insert":
            rows = self.payload if isinstance(self.payload, list) else [self.payload]
            inserted = []
            for r in rows:
                row = dict(r)
                if "id" not in row and self.table_name not in ("wallets", "billed_deals"):
                    row["id"] = str(uuid.uuid4())
                row.setdefault("created_at", now_iso())
                row["_seq"] = len(table)
                table.append(row)
                inserted.append({k: v for k, v in row.items() if k != "_seq"})
            return FakeResponse(inserted)
        if self.op == "update":
            updated = []
            for row in table:
                if self._matches(row):
                    row.update(self.payload)
                    updated.append({k: v for k, v in row.items() if k != "_seq"})
            return FakeResponse(updated)
        if self.op == "upsert":
            key = self.on_conflict
            for row in table:
                if row.get(key) == self.payload.get(key):
                    row.update(self.payload)
                    return FakeResponse([{k: v for k, v in row.items() if k != "_seq"}])
            row = dict(self.payload)
            row["_seq"] = len(table)
            table.append(row)
            return FakeResponse([{k: v for k, v in row.items() if k != "_seq"}])
        # select
        rows = [dict(r) for r in table if self._matches(r)]
        if self.order_col:
            rows.sort(key=lambda r: (r.get(self.order_col) is None, r.get(self.order_col), r.get("_seq", 0)), reverse=self.order_desc)
        if self.limit_n is not None:
            rows = rows[: self.limit_n]
        rows = [{k: v for k, v in r.items() if k != "_seq"} for r in rows]
        return FakeResponse(rows)


class FakeSupabase:
    def __init__(self):
        self.store: dict[str, list[dict]] = {}

    def table(self, name: str) -> FakeQuery:
        return FakeQuery(self.store, name)


class FakeFanout:
    """Minimal stand-in for FanoutCore - empty registries are fine for
    these tests, they only need pause/close's LifecycleError-on-missing-
    agent path to be exercised, not a real agent."""

    def __init__(self):
        self.master_agents: dict = {}
        self.follower_agents: dict = {}