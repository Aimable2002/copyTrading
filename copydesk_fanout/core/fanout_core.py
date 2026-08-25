from __future__ import annotations

import logging
import time
from typing import Any

from . import copy_events
from .config_store import ConfigStore
from .follower_agent import FollowerAgent
from .mt5_terminal import OrderCapExceeded
from .order_pair_store import OrderPairStore
from .sizing import calculate_follower_volume
from .terminal_agent import TerminalAgent

logger = logging.getLogger("fanout_core")

_MIN_PARTIAL_CLOSE_LOTS = 0.01

_MAX_CLOSE_RETRY_ATTEMPTS = 5


_INLINE_CLOSE_RETRY_ATTEMPTS = 4
_INLINE_CLOSE_RETRY_DELAY_SECONDS = 0.2


class FanoutCore:

    def __init__(self, config_store: ConfigStore, pair_store: OrderPairStore, supabase_client: Any | None = None):
        self.config_store = config_store
        self.pair_store = pair_store
        # Optional - None in local-file/no-DB mode. Used only for the
        # copy_events audit log (see core/copy_events.py); never required
        # for a copy to succeed.
        self.supabase_client = supabase_client
        self.master_agents: dict[str, TerminalAgent] = {}
        self.follower_agents: dict[str, FollowerAgent] = {}
        # (master_account_id, master_ticket) -> set of follower_account_id
        # whose close is still outstanding - see _attempt_close/retry_failed_closes.
        self._pending_close_retries: dict[tuple[str, str], set[str]] = {}
        self._close_retry_attempts: dict[tuple[str, str], int] = {}

    def register_master(self, agent: TerminalAgent) -> None:
        self.master_agents[agent.account_id] = agent

    def register_follower(self, agent: FollowerAgent) -> None:
        self.follower_agents[agent.account_id] = agent

    def unregister_master(self, account_id: str) -> None:
        self.master_agents.pop(account_id, None)

    def unregister_follower(self, account_id: str) -> None:
        self.follower_agents.pop(account_id, None)

    # ------------------------------------------------------------------ #
    # Startup reconciliation - crash/restart/deploy recovery
    # ------------------------------------------------------------------ #
    def reconcile_all(self) -> None:
        for master_account_id, master_agent in self.master_agents.items():
            expected = self.pair_store.get_open_master_tickets(master_account_id)
            result = master_agent.reconcile(expected)

            for master_ticket in result.closed_while_offline:
                logger.warning(
                    "Auto-closing follower copies of master %s#%s - closed while this process was offline",
                    master_account_id, master_ticket,
                )
                self._fan_out_close(master_account_id, master_ticket)

            for master_ticket in result.new_while_offline:
                logger.info(
                    "Catching up on master %s#%s - opened while this process was offline",
                    master_account_id, master_ticket,
                )
        for follower_account_id, follower_agent in self.follower_agents.items():
            expected = self.pair_store.get_expected_follower_tickets(follower_account_id)
            follower_agent.reconcile(expected)

    # ------------------------------------------------------------------ #
    # Master side
    # ------------------------------------------------------------------ #
    def handle_master_trade_event(self, master_account_id: str, event_type: str, ticket: str, order: dict) -> None:
        if event_type == "opened":
            self._fan_out_open(master_account_id, ticket, order)
        elif event_type == "closed":
            self._fan_out_close(master_account_id, ticket)
        elif event_type == "modified":
            self._fan_out_modify(master_account_id, ticket, order)
        elif event_type == "partial_closed":
            self._fan_out_partial_close(master_account_id, ticket, order)
        elif event_type == "pending_placed":
            self._fan_out_pending_open(master_account_id, ticket, order)
        elif event_type == "pending_cancelled":
            self._fan_out_pending_cancel(master_account_id, ticket)

    def _fan_out_open(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        # Marks the moment our system started processing this master fill -
        # the honest origin point for a "relay latency" metric. Not the
        # master's broker-side execution timestamp (we don't control clock
        # sync across different brokers' servers), but the time between us
        # detecting the fill and confirming each follower's copy - i.e. our
        # own pipeline's dispatch speed, which is what "relay latency"
        # actually means for this platform. See copy_events.record_success().
        dispatch_started_at = time.time()

        master_agent = self.master_agents.get(master_account_id)
        if master_agent is None:
            logger.warning("Trade event for unknown master account %s", master_account_id)
            return

        subscriptions = self.config_store.get_followers(master_account_id)
        if not subscriptions:
            logger.info("Master %s opened %s but has no active followers", master_account_id, master_ticket)
            return

        master_balance = master_agent.balance
        master_lots = master_order["lots"]

        for sub in subscriptions:
            follower_agent = self.follower_agents.get(sub.follower_account_id)
            if follower_agent is None:
                logger.warning("Subscribed follower %s has no registered agent", sub.follower_account_id)
                copy_events.record_failure(
                    master_account_id=master_account_id, follower_account_id=sub.follower_account_id,
                    master_ticket=master_ticket, reason="no_registered_agent", supabase_client=self.supabase_client,
                )
                continue
            if not follower_agent.is_connected:
                logger.warning("Follower %s not connected yet, skipping this fill", sub.follower_account_id)
                copy_events.record_failure(
                    master_account_id=master_account_id, follower_account_id=sub.follower_account_id,
                    master_ticket=master_ticket, reason="not_connected", supabase_client=self.supabase_client,
                )
                continue

            try:
                lots = calculate_follower_volume(
                    mode=sub.sizing_mode,
                    master_lots=master_lots,
                    master_balance=master_balance,
                    follower_balance=follower_agent.balance,
                    follower_equity=follower_agent.equity,
                    sizing_value=sub.sizing_value,
                )
            except ValueError as exc:
                logger.exception("Sizing failed for follower %s, skipping", sub.follower_account_id)
                copy_events.record_failure(
                    master_account_id=master_account_id, follower_account_id=sub.follower_account_id,
                    master_ticket=master_ticket, reason=f"sizing_failed: {exc}", supabase_client=self.supabase_client,
                )
                continue

            self.pair_store.add_pending(master_account_id, master_ticket, sub.follower_account_id, dispatched_lots=lots)
            try:
                result = follower_agent.execute_open(
                    master_ticket=master_ticket,
                    symbol=master_order["symbol"],
                    order_type=master_order["type"],
                    lots=lots,
                    stop_loss=master_order.get("SL", 0),
                    take_profit=master_order.get("TP", 0),
                )
            except OrderCapExceeded:
                logger.exception(
                    "Follower %s hit its order cap - skipping this copy, other followers unaffected",
                    sub.follower_account_id,
                )
                copy_events.record_failure(
                    master_account_id=master_account_id, follower_account_id=sub.follower_account_id,
                    master_ticket=master_ticket, reason="order_cap_exceeded", supabase_client=self.supabase_client,
                )
                continue

            if not result.get("success"):
                logger.warning(
                    "order_send rejected for follower %s: retcode=%s comment=%s",
                    sub.follower_account_id, result.get("retcode"), result.get("comment"),
                )
                print(" logging the entire results of order attempted :", result)
                copy_events.record_failure(
                    master_account_id=master_account_id, follower_account_id=sub.follower_account_id,
                    master_ticket=master_ticket,
                    reason=f"order_rejected: retcode={result.get('retcode')} comment={result.get('comment')}",
                    supabase_client=self.supabase_client,
                )
                continue

            logger.info(
                "Dispatched copy: master %s#%s -> follower %s, %.2f lots",
                master_account_id, master_ticket, sub.follower_account_id, lots,
            )
            copy_events.record_success(
                master_account_id=master_account_id, follower_account_id=sub.follower_account_id,
                master_ticket=master_ticket, lots=lots, supabase_client=self.supabase_client,
                latency_ms=(time.time() - dispatch_started_at) * 1000,
            )

    def _fan_out_pending_open(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        master_agent = self.master_agents.get(master_account_id)
        if master_agent is None:
            logger.warning("Pending-order event for unknown master account %s", master_account_id)
            return

        subscriptions = self.config_store.get_followers(master_account_id)
        if not subscriptions:
            logger.info("Master %s placed pending %s but has no active followers", master_account_id, master_ticket)
            return

        master_balance = master_agent.balance
        master_lots = master_order["lots"]

        for sub in subscriptions:
            follower_agent = self.follower_agents.get(sub.follower_account_id)
            if follower_agent is None:
                logger.warning("Subscribed follower %s has no registered agent", sub.follower_account_id)
                continue
            if not follower_agent.is_connected:
                logger.warning("Follower %s not connected yet, skipping this pending order", sub.follower_account_id)
                continue

            try:
                lots = calculate_follower_volume(
                    mode=sub.sizing_mode, master_lots=master_lots,
                    master_balance=master_balance, follower_balance=follower_agent.balance,
                    follower_equity=follower_agent.equity, sizing_value=sub.sizing_value,
                )
            except ValueError:
                logger.exception("Sizing failed for follower %s, skipping", sub.follower_account_id)
                continue

            self.pair_store.add_pending(master_account_id, master_ticket, sub.follower_account_id, dispatched_lots=lots)

            try:
                result = follower_agent.execute_open_pending(
                    master_ticket=master_ticket, symbol=master_order["symbol"],
                    order_type=master_order["type"], lots=lots,
                    price=master_order["open_price"],
                    stop_loss=master_order.get("SL", 0), take_profit=master_order.get("TP", 0),
                )
            except OrderCapExceeded:
                logger.exception(
                    "Follower %s hit its order cap - skipping this pending copy, other followers unaffected",
                    sub.follower_account_id,
                )
                self.pair_store.remove_pending(master_account_id, master_ticket, sub.follower_account_id)
                continue

            if not result.get("success") or not result.get("order"):
                logger.warning(
                    "Pending order_send rejected for follower %s: retcode=%s comment=%s",
                    sub.follower_account_id, result.get("retcode"), result.get("comment"),
                )
                self.pair_store.remove_pending(master_account_id, master_ticket, sub.follower_account_id)
                continue

            self.pair_store.add_pending_order(
                master_account_id, master_ticket, sub.follower_account_id,
                follower_ticket=str(result["order"]), dispatched_lots=lots,
            )
            logger.info(
                "Dispatched pending copy: master %s#%s -> follower %s#%s, %.2f lots at %.5f (exact)",
                master_account_id, master_ticket, sub.follower_account_id, result["order"], lots, master_order["open_price"],
            )

    def _fan_out_pending_cancel(self, master_account_id: str, master_ticket: str) -> None:
        followers = self.pair_store.get_pending_order_followers(master_account_id, master_ticket)
        if not followers:
            logger.info("Master %s cancelled pending %s but no follower copies were tracked", master_account_id, master_ticket)
            return

        for follower_account_id, copy in followers.items():
            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                continue
            result = follower_agent.execute_cancel_pending(follower_ticket=copy.follower_ticket)
            if not result.get("success"):
                logger.warning(
                    "Cancel FAILED for follower %s#%s (master %s#%s cancelled) - retcode=%s comment=%s - "
                    "left resting; not currently covered by the close-retry sweep, needs manual attention",
                    follower_account_id, copy.follower_ticket, master_account_id, master_ticket,
                    result.get("retcode"), result.get("comment"),
                )
                continue
            logger.info(
                "Cancelled pending copy: follower %s#%s (master %s#%s cancelled)",
                follower_account_id, copy.follower_ticket, master_account_id, master_ticket,
            )
            self.pair_store.remove_pending(master_account_id, master_ticket, follower_account_id)

        self.pair_store.remove_pending_order(master_account_id, master_ticket)

    def _fan_out_close(self, master_account_id: str, master_ticket: str) -> None:
        fills = self.pair_store.get_follower_fills(master_account_id, master_ticket)
        if not fills:
            logger.info("Master %s closed %s but no confirmed follower fills to close", master_account_id, master_ticket)
            return

        self._attempt_close(master_account_id, master_ticket, fills)

    def _attempt_close(self, master_account_id: str, master_ticket: str, fills: dict) -> None:
        key = (master_account_id, master_ticket)
        failed_followers: set[str] = set()

        for follower_account_id, fill in fills.items():
            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                continue

            result = follower_agent.execute_close(follower_ticket=fill.ticket)
            inline_attempt = 1
            while not result.get("success") and inline_attempt < _INLINE_CLOSE_RETRY_ATTEMPTS:
                logger.warning(
                    "Close rejected for follower %s#%s (master %s#%s closed) - retcode=%s comment=%s - "
                    "retrying immediately (inline attempt %d/%d)",
                    follower_account_id, fill.ticket, master_account_id, master_ticket,
                    result.get("retcode"), result.get("comment"),
                    inline_attempt + 1, _INLINE_CLOSE_RETRY_ATTEMPTS,
                )
                time.sleep(_INLINE_CLOSE_RETRY_DELAY_SECONDS)
                result = follower_agent.execute_close(follower_ticket=fill.ticket)
                inline_attempt += 1

            if not result.get("success"):
                failed_followers.add(follower_account_id)
                logger.warning(
                    "Close FAILED for follower %s#%s (master %s#%s closed) after %d immediate attempts - "
                    "retcode=%s comment=%s - handing off to the retry sweep rather than dropping it",
                    follower_account_id, fill.ticket, master_account_id, master_ticket,
                    inline_attempt, result.get("retcode"), result.get("comment"),
                )
                continue

            logger.info("Closed copy: follower %s#%s (master %s#%s closed)",
                        follower_account_id, fill.ticket, master_account_id, master_ticket)

        if failed_followers:
            attempts = self._close_retry_attempts.get(key, 0) + 1
            if attempts > _MAX_CLOSE_RETRY_ATTEMPTS:
                logger.error(
                    "Master %s#%s: follower(s) %s still failing to close after %d attempts - "
                    "giving up on auto-retry, this needs manual attention. Pairing stays tracked.",
                    master_account_id, master_ticket, sorted(failed_followers), attempts - 1,
                )
                self._pending_close_retries.pop(key, None)
                self._close_retry_attempts.pop(key, None)
                return
            self._close_retry_attempts[key] = attempts
            self._pending_close_retries[key] = failed_followers
        else:
            self._pending_close_retries.pop(key, None)
            self._close_retry_attempts.pop(key, None)
            self.pair_store.remove_master_trade(master_account_id, master_ticket)

    def retry_failed_closes(self) -> None:
        for key, follower_ids in list(self._pending_close_retries.items()):
            master_account_id, master_ticket = key
            fills = self.pair_store.get_follower_fills(master_account_id, master_ticket)
            retry_fills = {fid: fill for fid, fill in fills.items() if fid in follower_ids}
            if not retry_fills:
                self._pending_close_retries.pop(key, None)
                self._close_retry_attempts.pop(key, None)
                continue
            logger.info("Retrying failed close: master %s#%s -> %d follower(s)",
                        master_account_id, master_ticket, len(retry_fills))
            self._attempt_close(master_account_id, master_ticket, retry_fills)

    def _fan_out_modify(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        fills = self.pair_store.get_follower_fills(master_account_id, master_ticket)
        if not fills:
            logger.info("Master %s modified %s but no confirmed follower fills to update", master_account_id, master_ticket)
            return

        master_sl = master_order.get("SL", 0)
        master_tp = master_order.get("TP", 0)

        for follower_account_id, fill in fills.items():
            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                continue

            follower_agent.execute_modify(follower_ticket=fill.ticket, stop_loss=master_sl, take_profit=master_tp)
            logger.info(
                "Propagated SL/TP change: follower %s#%s -> SL=%.5f TP=%.5f (master %s#%s, exact)",
                follower_account_id, fill.ticket, master_sl, master_tp, master_account_id, master_ticket,
            )

    def _fan_out_partial_close(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        previous_lots = master_order.get("previous_lots", 0)
        new_lots = master_order.get("lots", 0)
        if not previous_lots or new_lots >= previous_lots:
            logger.warning("Partial close event for %s#%s had no usable lot reduction, skipping", master_account_id, master_ticket)
            return

        reduction_ratio = (previous_lots - new_lots) / previous_lots

        fills = self.pair_store.get_follower_fills(master_account_id, master_ticket)
        if not fills:
            logger.info("Master %s partially closed %s but no confirmed follower fills to reduce", master_account_id, master_ticket)
            return

        for follower_account_id, fill in fills.items():
            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                continue

            close_lots = round(fill.current_lots * reduction_ratio, 2)
            if close_lots < _MIN_PARTIAL_CLOSE_LOTS:
                logger.info(
                    "Follower %s partial-close reduction (%.2f lots) below minimum, skipping",
                    follower_account_id, close_lots,
                )
                continue

            follower_agent.execute_close(follower_ticket=fill.ticket, lots=close_lots)
            remaining = round(fill.current_lots - close_lots, 2)
            self.pair_store.record_partial_close(master_account_id, master_ticket, follower_account_id, new_lots=remaining)
            logger.info(
                "Propagated partial close: follower %s#%s closed %.2f lots (%.0f%% reduction, %.2f remaining)",
                follower_account_id, fill.ticket, close_lots, reduction_ratio * 100, remaining,
            )

    # ------------------------------------------------------------------ #
    # Follower side - fill confirmation for orders we dispatched
    # ------------------------------------------------------------------ #
    def handle_follower_trade_event(self, follower_account_id: str, event_type: str, ticket: str, order: dict) -> None:
        if event_type == "opened":
            comment = order.get("comment", "")
            if comment.startswith(FollowerAgent.COMMENT_PREFIX):
                master_ticket = comment[len(FollowerAgent.COMMENT_PREFIX):]
                confirmed_master_account_id = self.pair_store.confirm_fill(
                    follower_account_id, master_ticket, ticket,
                    open_price=order["open_price"], order_type=order["type"],
                )
                if confirmed_master_account_id:
                    logger.info("Confirmed fill: follower %s#%s <- master#%s at %.5f",
                                follower_account_id, ticket, master_ticket, order["open_price"])
    
                    self.pair_store.remove_pending_order(
                        confirmed_master_account_id, master_ticket, follower_account_id,
                    )
                else:
                    logger.warning(
                        "Follower %s got a new order tagged for master#%s but no pending copy was found "
                        "(comment tag mismatch or already confirmed)",
                        follower_account_id, master_ticket,
                    )
            else:
                logger.info("Follower %s opened %s manually (not a copy)", follower_account_id, ticket)

        elif event_type == "closed":
            master_ticket = self.pair_store.find_master_ticket_by_follower_ticket(follower_account_id, ticket)
            if master_ticket:
                logger.info(
                    "Follower %s#%s closed (was copying master#%s) - "
                    "not auto-reopening; this may have been our own close command completing, "
                    "or the follower closed it manually",
                    follower_account_id, ticket, master_ticket,
                )