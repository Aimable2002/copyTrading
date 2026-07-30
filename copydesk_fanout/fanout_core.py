from __future__ import annotations

import logging

from .config_store import ConfigStore
from .follower_agent import FollowerAgent
from .mt5_terminal import OrderCapExceeded
from .order_pair_store import OrderPairStore
from .sizing import calculate_follower_volume
from .sltp import apply_sl_tp_distance, sl_tp_distance
from .terminal_agent import TerminalAgent

logger = logging.getLogger("fanout_core")

# Below this, a partial-close reduction rounds to noise - just skip it
# rather than send a broker a close command for 0.00 lots.
_MIN_PARTIAL_CLOSE_LOTS = 0.01


class FanoutCore:
    """
    The actual new logic this whole build was for. Everything else
    (native MT5 execution via Mt5Terminal, the sizing formulas) is reused; this class is what
    connects them: master trade detected -> per-follower size computed ->
    dispatched -> pairing tracked so later closes/modifies/partial-closes on
    the master propagate to the right follower tickets.

    Holds master/follower agent references and config in memory. No
    database access anywhere in this class - that's the whole point.
    """

    def __init__(self, config_store: ConfigStore, pair_store: OrderPairStore):
        self.config_store = config_store
        self.pair_store = pair_store
        self.master_agents: dict[str, TerminalAgent] = {}
        self.follower_agents: dict[str, FollowerAgent] = {}

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
        """Call once at backend startup: AFTER pair_store.rebuild_from_supabase()
        (so we know what SHOULD be open) and BEFORE any agent.start() (so
        this runs before the live polling loops begin comparing against a
        real baseline instead of an empty one - see
        TerminalAgent.reconcile()'s docstring for the mechanics of why an
        unseeded baseline is dangerous on its own).

        This is the piece that actually protects against "losing track of
        an open trade" across a crash or restart:

        Master side is authoritative and acts immediately. If a master
        position closed while this process was down, the corresponding
        follower positions are real, live, un-hedged exposure sitting in
        someone's account with nothing watching it - so those get closed
        the moment we reconnect, no confirmation step, no delay. If a
        master position opened while this process was down, it's caught
        up the same way a live "opened" event would be handled - copied
        to followers now, late but correct.

        Follower side is seed-only and advisory. We deliberately do NOT
        auto-reopen a follower ticket that vanished while we were offline
        - we can't safely tell from here whether that was our own close
        command finishing, a manual close, or something else, and
        guessing wrong means placing a real order based on a guess. The
        master-side pass above is what actually protects the money; this
        pass just keeps each follower's polling baseline honest (so it
        doesn't misreport its own already-known positions as new) and
        logs any drift for a human to check.
        """
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
                # self._fan_out_open(master_account_id, master_ticket, result.live_orders[master_ticket])  this line is dangerous never uncomment this line or ....

        for follower_account_id, follower_agent in self.follower_agents.items():
            expected = self.pair_store.get_expected_follower_tickets(follower_account_id)
            follower_agent.reconcile(expected)
            # No auto-action here by design - see docstring above.

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

    def _fan_out_open(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        master_agent = self.master_agents.get(master_account_id)
        # print(" debugging masters order :", master_order)
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
                continue
            if not follower_agent.is_connected:
                logger.warning("Follower %s not connected yet, skipping this fill", sub.follower_account_id)
                continue

            try:
                lots = calculate_follower_volume(
                    mode=sub.sizing_mode,
                    master_lots=master_lots,
                    multiplier=sub.multiplier,
                    master_balance=master_balance,
                    follower_balance=follower_agent.balance,
                    fixed_master_balance=sub.fixed_master_balance,
                )
            except ValueError:
                logger.exception("Sizing failed for follower %s, skipping", sub.follower_account_id)
                continue

            self.pair_store.add_pending(master_account_id, master_ticket, sub.follower_account_id, dispatched_lots=lots)
            sl_distance, tp_distance = sl_tp_distance(
                order_type=master_order["type"],
                entry_price=master_order["open_price"],
                sl=master_order.get("SL", 0),
                tp=master_order.get("TP", 0),
            )
            try:
                result = follower_agent.execute_open(
                    master_ticket=master_ticket,
                    symbol=master_order["symbol"],
                    order_type=master_order["type"],
                    lots=lots,
                    sl_distance=sl_distance,
                    tp_distance=tp_distance,
                )
            except OrderCapExceeded:
                logger.exception(
                    "Follower %s hit its order cap - skipping this copy, other followers unaffected",
                    sub.follower_account_id,
                )
                continue

            if not result.get("success"):
                logger.warning(
                    "order_send rejected for follower %s: retcode=%s comment=%s",
                    sub.follower_account_id, result.get("retcode"), result.get("comment"),
                )
                print(" logging the entire results of order attempted :", result)
                continue

            logger.info(
                "Dispatched copy: master %s#%s -> follower %s, %.2f lots",
                master_account_id, master_ticket, sub.follower_account_id, lots,
            )

    def _fan_out_close(self, master_account_id: str, master_ticket: str) -> None:
        fills = self.pair_store.get_follower_fills(master_account_id, master_ticket)
        if not fills:
            logger.info("Master %s closed %s but no confirmed follower fills to close", master_account_id, master_ticket)
            return

        for follower_account_id, fill in fills.items():
            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                continue
            follower_agent.execute_close(follower_ticket=fill.ticket)
            logger.info("Closed copy: follower %s#%s (master %s#%s closed)",
                        follower_account_id, fill.ticket, master_account_id, master_ticket)

        self.pair_store.remove_master_trade(master_account_id, master_ticket)

    def _fan_out_modify(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        """
        Propagates an SL/TP change on the master's trade. Uses distance from
        the master's entry, reapplied to each follower's OWN entry price -
        not the master's absolute SL/TP price. See sltp.py for why.
        """
        fills = self.pair_store.get_follower_fills(master_account_id, master_ticket)
        if not fills:
            logger.info("Master %s modified %s but no confirmed follower fills to update", master_account_id, master_ticket)
            return

        sl_distance, tp_distance = sl_tp_distance(
            order_type=master_order["type"],
            entry_price=master_order["open_price"],
            sl=master_order.get("SL", 0),
            tp=master_order.get("TP", 0),
        )

        for follower_account_id, fill in fills.items():
            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                continue

            live_follower_order = follower_agent.terminal.open_orders.get(fill.ticket)
            follower_entry_price = (
                live_follower_order["open_price"] if live_follower_order and live_follower_order.get("open_price")
                else fill.open_price
            )
            if not follower_entry_price:
                logger.warning(
                    "Skipping SL/TP propagation for follower %s#%s - no settled entry price available yet",
                    follower_account_id, fill.ticket,
                )
                continue

            follower_sl, follower_tp = apply_sl_tp_distance(
                order_type=fill.order_type,
                entry_price=follower_entry_price,
                sl_distance=sl_distance,
                tp_distance=tp_distance,
            )
            follower_agent.execute_modify(follower_ticket=fill.ticket, stop_loss=follower_sl, take_profit=follower_tp)
            logger.info(
                "Propagated SL/TP change: follower %s#%s -> SL=%.5f TP=%.5f (master %s#%s distance-based)",
                follower_account_id, fill.ticket, follower_sl, follower_tp, master_account_id, master_ticket,
            )

    def _fan_out_partial_close(self, master_account_id: str, master_ticket: str, master_order: dict) -> None:
        """
        Propagates a partial close proportionally. The reduction ratio is
        computed from the MASTER's own before/after lots (carried directly
        on the event by TerminalAgent's modification poll), then applied to
        what we actually dispatched to each follower - not the master's
        absolute lot size, since a follower's position size is independently
        computed by the sizing mode and will usually differ from the
        master's.
        """
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
                confirmed = self.pair_store.confirm_fill(
                    follower_account_id, master_ticket, ticket,
                    open_price=order["open_price"], order_type=order["type"],
                )
                if confirmed:
                    logger.info("Confirmed fill: follower %s#%s <- master#%s at %.5f",
                                follower_account_id, ticket, master_ticket, order["open_price"])
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