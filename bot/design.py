"""
design.py
----------
Pure presentation layer for the Stop-and-Reverse (SAR) trading bot.

This file owns EVERYTHING visual: colors, the logo, the live dashboard,
the interactive setup wizard prompts, and log rendering. It knows nothing
about MT5 or trading math - it only ever reads a plain "state" dict handed
to it by core.py and renders it. This keeps design and logic fully
decoupled, as requested.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from rich.align import Align
from rich.box import ROUNDED, HEAVY
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.prompt import Confirm, FloatPrompt, IntPrompt, Prompt
from rich.table import Table
from rich.text import Text

# ----------------------------------------------------------------------
# THEME - single source of truth for the color palette
# ----------------------------------------------------------------------

class Theme:
    GOLD = "#D4AF37"
    GOLD_BRIGHT = "#FFD700"
    BG_ACCENT = "#1a1a1a"
    PROFIT = "#00E676"
    LOSS = "#FF5252"
    NEUTRAL = "#5DADE2"
    WARN = "#FFA726"
    DIM = "#7f8c8d"
    TEXT = "#ECECEC"
    BUY = "#00E676"
    SELL = "#FF5252"
    FLAT = "#7f8c8d"


LOGO = r"""
[bold {gold}]
     __  __ _____ ____    ____   ___  _____
    |  \/  |_   _| ___|  | __ ) / _ \|_   _|
    | |\/| | | | |___ \  |  _ \| | | | | |
    | |  | | | |  ___) | | |_) | |_| | | |
    |_|  |_| |_| |____/  |____/ \___/  |_|
[/bold {gold}]
[bold white]      S T O P  &  R E V E R S E   E N G I N E[/bold white]
""".strip("\n")


class TerminalUI:
    """Owns the console, the dashboard render loop, and the setup wizard."""

    def __init__(self) -> None:
        self.console = Console()

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def show_banner(self) -> None:
        self.console.clear()
        logo_text = LOGO.format(gold=Theme.GOLD)
        self.console.print(Align.center(logo_text))
        self.console.print(
            Align.center(
                Text(
                    "Automated Buy-Stop / Sell-Stop Reversal Scalper",
                    style=f"italic {Theme.DIM}",
                )
            )
        )
        self.console.print()

    # ------------------------------------------------------------------
    # First-run interactive setup wizard
    # ------------------------------------------------------------------

    def run_setup_wizard(self, existing: Dict[str, Any]) -> Dict[str, Any]:
        """Prompts the user for every configurable value. Nothing here is
        hardcoded into the bot logic - every answer is written to config.json."""
        cfg = existing

        self.console.print(Panel(
            "[bold]First-time setup[/bold]\n"
            "Answer the prompts below. Every value is saved to "
            "[bold]config.json[/bold] and can be edited later by hand or by "
            "re-running setup.",
            title="[bold gold1]SAR-BOT Configuration Wizard[/bold gold1]",
            border_style=Theme.GOLD,
            box=ROUNDED,
        ))
        self.console.print()

        # --- MT5 connection ---
        self.console.print(f"[bold {Theme.NEUTRAL}]── MT5 Terminal ──[/bold {Theme.NEUTRAL}]")
        cfg["mt5"]["terminal_path"] = Prompt.ask(
            "  Path to terminal64.exe",
            default=cfg["mt5"].get("terminal_path") or r"C:\Program Files\MetaTrader 5\terminal64.exe",
        )
        cfg["mt5"]["login"] = IntPrompt.ask(
            "  MT5 account login (0 = use already-logged-in terminal)",
            default=cfg["mt5"].get("login", 0),
        )
        if cfg["mt5"]["login"]:
            cfg["mt5"]["password"] = Prompt.ask("  MT5 password", password=True)
            cfg["mt5"]["server"] = Prompt.ask("  MT5 server", default=cfg["mt5"].get("server", ""))

        self.console.print()

        # --- Trading parameters ---
        self.console.print(f"[bold {Theme.NEUTRAL}]── Trading Parameters ──[/bold {Theme.NEUTRAL}]")
        cfg["trading"]["symbol"] = Prompt.ask(
            "  Symbol / pair", default=cfg["trading"].get("symbol", "XAUUSD.a")
        )
        cfg["trading"]["lot_size"] = FloatPrompt.ask(
            "  Lot size", default=cfg["trading"].get("lot_size", 0.10)
        )
        cfg["trading"]["contract_size"] = FloatPrompt.ask(
            "  Contract size per 1.0 lot (e.g. 100 for gold)",
            default=cfg["trading"].get("contract_size", 100),
        )
        cfg["trading"]["initial_stop_distance_percent"] = FloatPrompt.ask(
            "  Initial straddle distance (% of current price, e.g. 0.05 = 0.05%)",
            default=cfg["trading"].get("initial_stop_distance_percent", 0.05),
        )
        cfg["trading"]["spread_safety_multiplier"] = FloatPrompt.ask(
            "  Spread safety multiplier (min distance = spread x this, to avoid invalid-price rejections)",
            default=cfg["trading"].get("spread_safety_multiplier", 1.5),
        )
        cfg["trading"]["slippage_points"] = IntPrompt.ask(
            "  Max allowed slippage (points)",
            default=cfg["trading"].get("slippage_points", 20),
        )
        cfg["trading"]["magic_number"] = IntPrompt.ask(
            "  Magic number", default=cfg["trading"].get("magic_number", 990011)
        )

        self.console.print()

        # --- Profit protection: R-multiplier model ---
        self.console.print(f"[bold {Theme.NEUTRAL}]── Risk & Protection (R-multiplier model) ──[/bold {Theme.NEUTRAL}]")
        self.console.print(
            "  [dim]All three values below are multiples of a position's own risk unit (R), "
            "which is computed fresh for every trade from live spread - not a fixed pip count.[/dim]"
        )
        cfg["protection"]["stop_loss_r"] = FloatPrompt.ask(
            "  Hard stop-loss, in multiples of R (e.g. 1.0 = close if price moves 1R against entry)",
            default=cfg["protection"].get("stop_loss_r", 1.0),
        )
        cfg["protection"]["trail_arm_r"] = FloatPrompt.ask(
            "  Trail arm point, in multiples of R (trail starts protecting profit once reached)",
            default=cfg["protection"].get("trail_arm_r", 1.5),
        )
        cfg["protection"]["trail_buffer_r"] = FloatPrompt.ask(
            "  Trail buffer, in multiples of R (stop trails this far behind the best profit seen)",
            default=cfg["protection"].get("trail_buffer_r", 0.5),
        )

        self.console.print()

        # --- Risk management ---
        self.console.print(f"[bold {Theme.NEUTRAL}]── Daily Risk Limits ──[/bold {Theme.NEUTRAL}]")
        cfg["risk"]["max_daily_drawdown_usd"] = FloatPrompt.ask(
            "  Max daily drawdown (USD) before bot halts",
            default=cfg["risk"].get("max_daily_drawdown_usd", 200.0),
        )
        cfg["risk"]["daily_profit_target_usd"] = FloatPrompt.ask(
            "  Daily profit target (USD) before bot halts",
            default=cfg["risk"].get("daily_profit_target_usd", 300.0),
        )
        cfg["risk"]["stop_trading_on_drawdown_hit"] = Confirm.ask(
            "  Auto-stop when drawdown limit is hit?",
            default=cfg["risk"].get("stop_trading_on_drawdown_hit", True),
        )
        cfg["risk"]["stop_trading_on_target_hit"] = Confirm.ask(
            "  Auto-stop when profit target is hit?",
            default=cfg["risk"].get("stop_trading_on_target_hit", True),
        )

        self.console.print()
        cfg["engine"]["poll_interval_seconds"] = FloatPrompt.ask(
            "  Engine poll interval (seconds)",
            default=cfg["engine"].get("poll_interval_seconds", 1.0),
        )

        cfg["setup_complete"] = True

        self.console.print()
        self.console.print(Panel(
            "Configuration saved to [bold]config.json[/bold].",
            border_style=Theme.PROFIT, box=ROUNDED,
        ))
        time.sleep(0.6)
        return cfg

    # ------------------------------------------------------------------
    # Live dashboard
    # ------------------------------------------------------------------

    def _side_style(self, side: str) -> str:
        return {"BUY": Theme.BUY, "SELL": Theme.SELL}.get(side, Theme.FLAT)

    def _pnl_style(self, value: float) -> str:
        if value > 0:
            return Theme.PROFIT
        if value < 0:
            return Theme.LOSS
        return Theme.NEUTRAL

    def build_dashboard(self, state: Dict[str, Any]) -> Group:
        connected = state.get("connected", False)
        header_style = Theme.GOLD if connected else Theme.LOSS
        conn_label = "● CONNECTED" if connected else "● DISCONNECTED"
        header = Panel(
            Align.center(
                Text(f"  {state.get('symbol', '—')}   ", style=f"bold {Theme.GOLD_BRIGHT}")
                + Text(conn_label, style=f"bold {Theme.PROFIT if connected else Theme.LOSS}")
            ),
            box=HEAVY, border_style=header_style, padding=(0, 1),
        )

        # Account panel
        acc_table = Table.grid(padding=(0, 2))
        acc_table.add_column(justify="right", style=Theme.DIM)
        acc_table.add_column(justify="left")
        if not state.get("mt5_available", True):
            acc_table.add_row("MT5 package", f"[bold {Theme.LOSS}]not installed[/bold {Theme.LOSS}]")
        acc_table.add_row(
            "Status",
            f"[bold {Theme.PROFIT}]connected[/bold {Theme.PROFIT}]" if connected
            else f"[bold {Theme.LOSS}]disconnected — retrying...[/bold {Theme.LOSS}]",
        )
        acc_table.add_row("Login", str(state.get("account_login") or "—"))
        acc_table.add_row("Srv", state.get("account_server") or "—")
        balance = state.get("account_balance", 0.0)
        currency = state.get("account_currency", "")
        acc_table.add_row("Bal", f"{balance:.2f} {currency}".strip() if connected else "—")

        # Positions panel - a list, not a single position, since a
        # hedging account can hold several independent positions at once
        positions = state.get("positions", [])
        pos_table = Table(box=None, expand=True, show_edge=False, pad_edge=False)
        pos_table.add_column("Side", style=Theme.DIM, width=6, no_wrap=True)
        pos_table.add_column("Ticket", style=Theme.DIM, width=12, no_wrap=True)
        pos_table.add_column("Entry", justify="right", width=12, no_wrap=True)
        pos_table.add_column("SL price", justify="right", width=12, no_wrap=True)
        pos_table.add_column("P/L (R)", justify="right", width=10, no_wrap=True)
        pos_table.add_column("P/L (USD)", justify="right", width=12, no_wrap=True)
        pos_table.add_column("Trail", justify="center", width=8, no_wrap=True)
        pos_table.add_column("Stop order", justify="center", no_wrap=True)
        if not positions:
            pos_table.add_row("—", "—", "—", "—", "—", "—", "—", "no open positions")
        for p in positions:
            side_style = self._side_style(p["side"])
            pnl_style = self._pnl_style(p["profit_r"])
            has_stop = p.get("has_stop_order")
            pos_table.add_row(
                f"[bold {side_style}]{p['side']}[/bold {side_style}]",
                str(p["ticket"]),
                f"{p['entry_price']:.2f}",
                f"{p['sl_price']:.2f}",
                f"[{pnl_style}]{p['profit_r']:+.2f}R[/{pnl_style}]",
                f"[{pnl_style}]${p['profit_usd']:+.2f}[/{pnl_style}]",
                f"[bold {Theme.PROFIT}]ARMED[/bold {Theme.PROFIT}]" if p.get("trail_armed") else "[dim]idle[/dim]",
                f"[{Theme.PROFIT}]● live[/{Theme.PROFIT}]" if has_stop else f"[bold {Theme.LOSS}]● MISSING[/bold {Theme.LOSS}]",
            )

        # Protection panel - per-position SL/trail state now lives in the
        # positions table above; this panel just shows the configured
        # R-multiplier thresholds and the overall pending-order summary.
        prot_table = Table.grid(padding=(0, 2))
        prot_table.add_column(justify="right", style=Theme.DIM)
        prot_table.add_column(justify="left")
        prot_table.add_row("Current price", f"{state.get('current_price', 0):.2f}")
        total_pnl = state.get("total_profit_usd", 0.0)
        prot_table.add_row(
            "Total open P/L",
            f"[bold {self._pnl_style(total_pnl)}]${total_pnl:+.2f}[/bold {self._pnl_style(total_pnl)}]",
        )
        prot_table.add_row("Stop-loss at", f"{state.get('stop_loss_r', 0)}R")
        prot_table.add_row("Trail arms at", f"{state.get('trail_arm_r', 0)}R")
        prot_table.add_row("Trail buffer", f"{state.get('trail_buffer_r', 0)}R")
        prot_table.add_row("Pending", state.get("pending_order_desc", "—"))

        # Risk panel
        risk_table = Table.grid(padding=(0, 2))
        risk_table.add_column(justify="right", style=Theme.DIM)
        risk_table.add_column(justify="left")
        day_pnl = state.get("daily_pnl_usd", 0.0)
        risk_table.add_row(
            "Daily P/L", f"[bold {self._pnl_style(day_pnl)}]${day_pnl:+.2f}[/bold {self._pnl_style(day_pnl)}]"
        )
        risk_table.add_row("Daily target", f"${state.get('daily_profit_target_usd', 0):.2f}")
        risk_table.add_row("Max drawdown", f"${state.get('max_daily_drawdown_usd', 0):.2f}")
        halted = state.get("trading_halted", False)
        risk_table.add_row(
            "Status",
            f"[bold {Theme.LOSS}]HALTED — {state.get('halt_reason', '')}[/bold {Theme.LOSS}]"
            if halted else f"[bold {Theme.PROFIT}]ACTIVE[/bold {Theme.PROFIT}]",
        )

        top_row = Table.grid(expand=True, padding=(0, 1))
        top_row.add_column(ratio=1)
        top_row.add_column(ratio=1)
        top_row.add_column(ratio=1)
        top_row.add_row(
            Panel(acc_table, title="Account", border_style=Theme.PROFIT if connected else Theme.LOSS, box=ROUNDED),
            Panel(prot_table, title="Protection", border_style=Theme.NEUTRAL, box=ROUNDED),
            Panel(risk_table, title="Risk", border_style=Theme.GOLD, box=ROUNDED),
        )

        positions_panel = Panel(
            pos_table,
            title=f"Positions ({len(positions)} open)",
            border_style=Theme.NEUTRAL, box=ROUNDED,
        )

        log_lines = state.get("log", [])[-8:]
        log_text = Text("\n".join(log_lines) or "waiting for events...", style=Theme.DIM)
        log_panel = Panel(log_text, title="Event Log", border_style=Theme.BG_ACCENT, box=ROUNDED)

        footer = Text(
            f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}   •   Ctrl+C to stop  ",
            style=Theme.DIM, justify="center",
        )

        return Group(header, top_row, positions_panel, log_panel, footer)

    # ------------------------------------------------------------------
    # Main render loop - pulls state from core.py via tick_fn each cycle
    # ------------------------------------------------------------------

    def run_live(self, tick_fn: Callable[[], Dict[str, Any]], refresh_per_second: float = 2.0) -> None:
        """tick_fn is provided by core.py: calling it advances the engine
        by one cycle and returns the latest state dict to render."""
        with Live(console=self.console, refresh_per_second=refresh_per_second, screen=True) as live:
            while True:
                state = tick_fn()
                live.update(self.build_dashboard(state))
                if state.get("shutdown"):
                    break

    def print_fatal(self, message: str) -> None:
        self.console.print(Panel(f"[bold {Theme.LOSS}]{message}[/bold {Theme.LOSS}]",
                                  title="Fatal Error", border_style=Theme.LOSS, box=HEAVY))

    def print_info(self, message: str) -> None:
        self.console.print(f"[{Theme.NEUTRAL}]{message}[/{Theme.NEUTRAL}]")