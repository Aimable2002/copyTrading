"""
main.py
--------
Entry point / interactive terminal. Wires design.py (presentation) to
core.py (trading logic) together. Run this file to start the bot:

    python main.py

On first run (or if config.json is missing setup_complete=true) it walks
you through an interactive setup wizard and writes config.json. On every
subsequent run it loads config.json directly and starts trading.
"""

from __future__ import annotations

import sys
import time

from rich.prompt import Confirm

import core
import design


CONFIG_PATH = "config.json"

DEFAULT_CONFIG = {
    "mt5": {"terminal_path": "", "login": 0, "password": "", "server": "", "timeout_ms": 10000},
    "trading": {
        "symbol": "XAUUSD.a", "lot_size": 0.10, "magic_number": 990011,
        "initial_stop_distance_percent": 0.05, "spread_safety_multiplier": 1.5,
        "contract_size": 100,
        "slippage_points": 20, "comment": "SAR-BOT",
    },
    "protection": {"stop_loss_r": 1.0, "trail_arm_r": 1.5, "trail_buffer_r": 0.5},
    "risk": {
        "max_daily_drawdown_usd": 200.0, "daily_profit_target_usd": 300.0,
        "stop_trading_on_drawdown_hit": True, "stop_trading_on_target_hit": True,
    },
    "engine": {"poll_interval_seconds": 1.0, "log_file": "sar_bot.log"},
    "setup_complete": False,
}


def load_or_create_config(ui: design.TerminalUI) -> dict:
    try:
        cfg = core.load_config(CONFIG_PATH)
    except FileNotFoundError:
        cfg = DEFAULT_CONFIG

    if not cfg.get("setup_complete", False):
        cfg = ui.run_setup_wizard(cfg)
        core.save_config(cfg, CONFIG_PATH)
    else:
        rerun = Confirm.ask(
            "[bold]config.json[/bold] found. Re-run setup wizard?", default=False
        )
        if rerun:
            cfg = ui.run_setup_wizard(cfg)
            core.save_config(cfg, CONFIG_PATH)

    return cfg


def main() -> int:
    ui = design.TerminalUI()
    ui.show_banner()

    cfg = load_or_create_config(ui)

    engine = core.SARTradeEngine(cfg)
    connected = engine.connect()
    if not connected:
        ui.print_info(
            "Running in SIMULATED mode (MT5 not available on this system or connection "
            "failed). The dashboard will show simulated log events so you can verify "
            "the state machine, but no real orders are sent."
        )
        time.sleep(1.5)

    def tick_fn():
        state = engine.tick()
        time.sleep(engine.poll_interval)
        return state

    try:
        ui.run_live(tick_fn)
    except KeyboardInterrupt:
        pass
    finally:
        engine.shutdown()
        ui.print_info("Bot stopped. Goodbye.")

    return 0


if __name__ == "__main__":
    sys.exit(main())