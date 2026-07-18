from __future__ import annotations

import logging

from .config_store import ConfigStore
from .follower_agent import FollowerAgent
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
    (DWX Connect, the sizing formulas) is reused; this class is what
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
            # SL/TP intentionally NOT passed here (0/0 - open cleanly first).
            # We don't know the follower's real fill price yet (DWX Connect's
            # bridge is async), so distance-based SL/TP is computed and
            # applied once the fill is confirmed - see handle_follower_trade_event.
            follower_agent.execute_open(
                master_ticket=master_ticket,
                symbol=master_order["symbol"],
                order_type=master_order["type"],
                lots=lots,
            )
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

            follower_sl, follower_tp = apply_sl_tp_distance(
                order_type=fill.order_type,
                entry_price=fill.open_price,
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
                    self._apply_initial_sl_tp(follower_account_id, master_ticket, ticket, order)
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

    def _apply_initial_sl_tp(self, follower_account_id: str, master_ticket: str, follower_ticket: str, follower_order: dict) -> None:
        """
        Called right after a follower's fill is confirmed - applies the
        master's SL/TP distance to the follower's real fill price. Reads
        the master's current order state directly off the registered master
        agent (whichever one has this ticket), since that's simpler and more
        current than threading the master's order dict through the whole
        pending/confirm flow.
        """
        for master_account_id, master_agent in self.master_agents.items():
            master_order = master_agent._last_orders.get(master_ticket)  # noqa: SLF001 - internal, same module family
            if master_order is None:
                continue

            follower_agent = self.follower_agents.get(follower_account_id)
            if follower_agent is None:
                return

            sl_distance, tp_distance = sl_tp_distance(
                order_type=master_order["type"],
                entry_price=master_order["open_price"],
                sl=master_order.get("SL", 0),
                tp=master_order.get("TP", 0),
            )
            if not sl_distance and not tp_distance:
                return  # master trade has no SL/TP set - nothing to inherit

            follower_sl, follower_tp = apply_sl_tp_distance(
                order_type=follower_order["type"],
                entry_price=follower_order["open_price"],
                sl_distance=sl_distance,
                tp_distance=tp_distance,
            )
            follower_agent.execute_modify(follower_ticket=follower_ticket, stop_loss=follower_sl, take_profit=follower_tp)
            logger.info(
                "Applied initial distance-based SL/TP: follower %s#%s -> SL=%.5f TP=%.5f",
                follower_account_id, follower_ticket, follower_sl, follower_tp,
            )
            return
