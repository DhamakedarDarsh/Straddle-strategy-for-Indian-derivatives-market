"""
============================================================
NIFTY Options Straddle Trading Bot
============================================================
Strategy  : Momentum-filtered straddle (Buy CE + PE)
Index     : NIFTY (Weekly expiry)
Entry     : 09:17 – 15:15 IST, after momentum confirmation
Target    : +30 % per leg
Stop Loss : –20 % per leg
Trail SL  : 10-point trail on each leg
Momentum  : Simple momentum ≥ 15 % over the last candle before entry
Data src  : AlgoTest backtest (Feb-Mar 2026, 36 trades)
============================================================
"""

# ─── Standard Library ────────────────────────────────────────
import os
import sys
import time
import json
import logging
import threading
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

# ─── Third-party  ────────────────────────────────────────────
# pip install smartapi-python logzero pyotp requests websocket-client
try:
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except ImportError:
    sys.exit(
        "[ERROR] SmartAPI package not found.\n"
        "  Run: pip install smartapi-python"
    )

import pyotp          # pip install pyotp
import requests

# ─── Logging Setup ───────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("straddle_bot.log"),
    ],
)
log = logging.getLogger("StraddleBot")

# ═══════════════════════════════════════════════════════════════
#  1. CONFIGURATION  — edit these before running
# ═══════════════════════════════════════════════════════════════
class Config:
    # ── Platform credentials ──────────────────────────────────
    API_KEY        = 
    CLIENT_ID      =
    PASSWORD       =   # login PIN
    TOTP_SECRET    = # base-32 seed

    # ── Instrument ───────────────────────────────────────────
    UNDERLYING     = "NIFTY"
    EXCHANGE       = "NFO"                # NSE F&O segment
    PRODUCT_TYPE   = "INTRADAY"           # INTRADAY / DELIVERY / CARRYFORWARD
    ORDER_TYPE     = "MARKET"             # MARKET / LIMIT
    VARIETY        = "NORMAL"

    # ── Quantity / lots ───────────────────────────────────────
    # From backtest CSV: qty = 130 shares per leg
    # NIFTY lot size as of 2026 = 75; 130 ≈ ~2 lots adjusted.
    # Change to match your actual lot size * number of lots.
    QUANTITY       = 75                   # shares per leg (1 lot)
    NUM_LOTS       = 2                    # number of lots per leg

    # ── Strike selection ─────────────────────────────────────
    # "Closest Premium" → find CE/PE whose LTP is nearest ₹25
    TARGET_PREMIUM = 25.0                 # target option LTP (₹)

    # ── Timing (IST) ─────────────────────────────────────────
    TZ             = ZoneInfo("Asia/Kolkata")
    ENTRY_START    = (9, 17)              # hh, mm — earliest entry
    ENTRY_CUTOFF   = (15, 15)            # hh, mm — no new entry after this
    MARKET_CLOSE   = (15, 30)            # force-square-off time

    # ── Strategy parameters ───────────────────────────────────
    TARGET_PCT     = 30.0                 # % profit target per leg
    STOPLOSS_PCT   = 20.0                 # % stop-loss per leg
    TRAIL_POINTS   = 10.0                 # trailing SL step (₹ points)
    MOMENTUM_PCT   = 15.0                 # min % momentum to trigger entry
    POLL_SECONDS   = 5                    # LTP polling interval (seconds)

    # ── Misc ─────────────────────────────────────────────────
    DRY_RUN        = True                 # True = paper trade, no real orders
    MAX_DAILY_LOSS = -5000                # ₹ — daily loss limit (0 = disabled)

# ═══════════════════════════════════════════════════════════════
#  2. AUTHENTICATION
# ═══════════════════════════════════════════════════════════════

# Platform authentication details

# ═══════════════════════════════════════════════════════════════
#  3. INSTRUMENT LOOKUP
# ═══════════════════════════════════════════════════════════════
class InstrumentManager:
    """
    Downloads the AngelOne instrument master (JSON) and provides
    helpers to look up NIFTY option tokens/symbols.
    """
    MASTER_URL = (
        "https://margincalculator.angelbroking.com/OpenAPI_File/"
        "files/OpenAPIScripMaster.json"
    )

    def __init__(self):
        self.instruments: list[dict] = []
        self._load_master()

    def _load_master(self):
        log.info("Downloading instrument master …")
        try:
            resp = requests.get(self.MASTER_URL, timeout=30)
            resp.raise_for_status()
            self.instruments = resp.json()
            log.info("Loaded %d instruments", len(self.instruments))
        except Exception as exc:
            log.exception("Could not download instrument master: %s", exc)

    def get_weekly_expiry(self) -> date:
        """Return the nearest Thursday (weekly NIFTY expiry)."""
        today = date.today()
        days_ahead = (3 - today.weekday()) % 7   # 3 = Thursday
        if days_ahead == 0:
            # Already Thursday — check if expiry time has passed
            now = datetime.now(Config.TZ)
            if now.hour >= 15 and now.minute >= 30:
                days_ahead = 7
        return today + timedelta(days=days_ahead)

    def find_option(
        self,
        expiry: date,
        option_type: str,   # "CE" or "PE"
        strike: float,
    ) -> dict | None:
        """
        Return the instrument dict for the requested option.
        option_type : 'CE' or 'PE'
        strike      : strike price (e.g. 25000)
        """
        expiry_str = expiry.strftime("%d%b%Y").upper()  # e.g. 06FEB2026
        for inst in self.instruments:
            if (
                inst.get("name")       == f"NIFTY"
                and inst.get("exch_seg")  == "NFO"
                and inst.get("instrumenttype") in ("OPTIDX",)
                and inst.get("expiry")    == expiry_str
                and inst.get("symbol", "").endswith(option_type)
                and float(inst.get("strike", 0)) / 100 == strike
            ):
                return inst
        return None

    def find_closest_premium_strike(
        self,
        api: SmartConnect,
        expiry: date,
        option_type: str,
        target_premium: float,
        spot: float,
        search_range: int = 20,
    ) -> dict | None:
        """
        Scan strikes around ATM and return the instrument whose
        LTP is closest to target_premium.
        """
        # Round spot to nearest 50
        atm = round(spot / 50) * 50
        # Build candidate strikes: ATM ± search_range * 50
        candidates = [
            atm + i * 50 for i in range(-search_range, search_range + 1)
        ]

        best_inst  = None
        best_delta = float("inf")

        for strike in candidates:
            inst = self.find_option(expiry, option_type, strike)
            if not inst:
                continue
            ltp = self._get_ltp(api, inst)
            if ltp is None:
                continue
            delta = abs(ltp - target_premium)
            if delta < best_delta:
                best_delta = delta
                best_inst  = inst
                best_inst["_ltp"] = ltp   # cache LTP

        return best_inst

    @staticmethod
    def _get_ltp(api: SmartConnect, inst: dict) -> float | None:
        try:
            resp = api.ltpData(
                inst["exch_seg"],
                inst["symbol"],
                inst["token"],
            )
            if resp["status"]:
                return float(resp["data"]["ltp"])
        except Exception:
            pass
        return None

# ═══════════════════════════════════════════════════════════════
#  4. MOMENTUM FILTER
# ═══════════════════════════════════════════════════════════════
class MomentumFilter:
    """
    Simple momentum: compare current NIFTY spot to the price
    observed MOMENTUM_LOOKBACK_MIN minutes ago.
    Momentum % = (current - old) / old * 100
    Entry is allowed when abs(momentum) >= MOMENTUM_PCT.
    """
    MOMENTUM_LOOKBACK_MIN = 5    # candle lookback (minutes)

    def __init__(self, cfg: Config):
        self.cfg        = cfg
        self._price_log: list[tuple[datetime, float]] = []

    def record_price(self, price: float):
        now = datetime.now(self.cfg.TZ)
        self._price_log.append((now, price))
        # Keep only last 30 minutes
        cutoff = now - timedelta(minutes=30)
        self._price_log = [
            (t, p) for t, p in self._price_log if t >= cutoff
        ]

    def is_momentum_confirmed(self, current_price: float) -> tuple[bool, float]:
        """
        Returns (confirmed, momentum_pct).
        Confirmed = True when abs(momentum_pct) >= cfg.MOMENTUM_PCT.
        """
        now     = datetime.now(self.cfg.TZ)
        lookback = now - timedelta(minutes=self.MOMENTUM_LOOKBACK_MIN)
        old_prices = [
            p for t, p in self._price_log if t <= lookback
        ]
        if not old_prices:
            return False, 0.0

        ref_price    = old_prices[-1]
        momentum_pct = (current_price - ref_price) / ref_price * 100
        confirmed    = abs(momentum_pct) >= self.cfg.MOMENTUM_PCT

        log.debug(
            "Momentum: ref=%.2f  now=%.2f  Δ=%.2f%%  required=%.1f%%  → %s",
            ref_price, current_price, momentum_pct,
            self.cfg.MOMENTUM_PCT, "✓" if confirmed else "✗",
        )
        return confirmed, momentum_pct

# ═══════════════════════════════════════════════════════════════
#  5. ORDER MANAGER
# ═══════════════════════════════════════════════════════════════
class OrderManager:
    """Place, monitor and square-off option legs."""

    def __init__(self, session: AngelSession, cfg: Config):
        self.session = session
        self.api     = session.api
        self.cfg     = cfg

    # ── Place order ──────────────────────────────────────────
    def place_order(
        self,
        symbol:        str,
        token:         str,
        transaction:   str,    # "BUY" or "SELL"
        qty:           int,
        price:         float = 0,
    ) -> str | None:
        """
        Places a market/limit order.  Returns order_id or None.
        """
        order_params = {
            "variety":          self.cfg.VARIETY,
            "tradingsymbol":    symbol,
            "symboltoken":      token,
            "transactiontype":  transaction,
            "exchange":         self.cfg.EXCHANGE,
            "ordertype":        self.cfg.ORDER_TYPE,
            "producttype":      self.cfg.PRODUCT_TYPE,
            "duration":         "DAY",
            "price":            str(price),
            "squareoff":        "0",
            "stoploss":         "0",
            "quantity":         str(qty),
        }

        if self.cfg.DRY_RUN:
            fake_id = f"DRY-{symbol}-{int(time.time())}"
            log.info(
                "[DRY RUN] %s %s × %d  @ %s  → %s",
                transaction, symbol, qty,
                "MARKET" if price == 0 else f"₹{price:.2f}",
                fake_id,
            )
            return fake_id

        try:
            resp = self.api.placeOrder(order_params)
            if resp["status"]:
                oid = resp["data"]["orderid"]
                log.info(
                    "✅  Order placed: %s %s × %d → order_id=%s",
                    transaction, symbol, qty, oid,
                )
                return oid
            log.error("Order failed: %s", resp.get("message"))
        except Exception as exc:
            log.exception("Place order exception: %s", exc)
        return None

    # ── Get LTP ──────────────────────────────────────────────
    def get_ltp(self, exchange: str, symbol: str, token: str) -> float | None:
        if self.cfg.DRY_RUN:
            # Simulate small random price movement in dry-run
            import random
            return round(25 + random.uniform(-5, 5), 2)

        try:
            resp = self.api.ltpData(exchange, symbol, token)
            if resp["status"]:
                return float(resp["data"]["ltp"])
        except Exception as exc:
            log.warning("LTP fetch error: %s", exc)
        return None

    # ── Get NIFTY spot ────────────────────────────────────────
    def get_nifty_spot(self) -> float | None:
        if self.cfg.DRY_RUN:
            import random
            return round(23000 + random.uniform(-100, 100), 2)
        try:
            resp = self.api.ltpData("NSE", "NIFTY", "26000")
            if resp["status"]:
                return float(resp["data"]["ltp"])
        except Exception as exc:
            log.warning("NIFTY spot error: %s", exc)
        return None

# ═══════════════════════════════════════════════════════════════
#  6. LEG TRACKER  — tracks P&L, SL, target, trailing SL
# ═══════════════════════════════════════════════════════════════
class Leg:
    """Represents a single option leg (CE or PE)."""

    def __init__(
        self,
        option_type:    str,      # "CE" or "PE"
        symbol:         str,
        token:          str,
        strike:         float,
        entry_price:    float,
        quantity:       int,
        order_id:       str,
        cfg:            Config,
    ):
        self.option_type  = option_type
        self.symbol       = symbol
        self.token        = token
        self.strike       = strike
        self.entry_price  = entry_price
        self.quantity     = quantity
        self.order_id     = order_id
        self.cfg          = cfg

        self.target_price = round(entry_price * (1 + cfg.TARGET_PCT / 100), 2)
        self.sl_price     = round(entry_price * (1 - cfg.STOPLOSS_PCT / 100), 2)
        self.trail_high   = entry_price       # highest LTP seen
        self.trail_sl     = self.sl_price     # trailing SL (moves up)
        self.is_open      = True
        self.exit_price   = None
        self.exit_reason  = None

        log.info(
            "📌  Leg opened: %s | %s | entry=₹%.2f | target=₹%.2f | SL=₹%.2f",
            option_type, symbol, entry_price, self.target_price, self.sl_price,
        )

    def update(self, ltp: float) -> str | None:
        """
        Feed latest LTP.  Returns exit reason string if leg should be closed,
        else None.
        """
        if not self.is_open:
            return None

        # ── Update trailing SL ────────────────────────────────
        if ltp > self.trail_high:
            self.trail_high = ltp
            new_trail_sl = self.trail_high - self.cfg.TRAIL_POINTS
            if new_trail_sl > self.trail_sl:
                old = self.trail_sl
                self.trail_sl = new_trail_sl
                log.debug(
                    "Trail SL moved: %s %.2f → %.2f",
                    self.option_type, old, self.trail_sl,
                )

        pnl_pct = (ltp - self.entry_price) / self.entry_price * 100

        # ── Target hit ────────────────────────────────────────
        if ltp >= self.target_price:
            return f"TARGET ({pnl_pct:+.1f}%)"

        # ── Trailing SL hit ───────────────────────────────────
        if ltp <= self.trail_sl and self.trail_high > self.entry_price:
            return f"TRAIL_SL ({pnl_pct:+.1f}%)"

        # ── Hard SL hit ───────────────────────────────────────
        if ltp <= self.sl_price:
            return f"STOP_LOSS ({pnl_pct:+.1f}%)"

        return None

    def close(self, exit_price: float, reason: str):
        self.is_open    = False
        self.exit_price = exit_price
        self.exit_reason = reason
        pnl = (exit_price - self.entry_price) * self.quantity
        log.info(
            "🔴  Leg closed: %s | %s | exit=₹%.2f | reason=%s | P&L=₹%.2f",
            self.option_type, self.symbol, exit_price, reason, pnl,
        )

    @property
    def realised_pnl(self) -> float:
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) * self.quantity

# ═══════════════════════════════════════════════════════════════
#  7. TRADE  — groups one CE + one PE leg
# ═══════════════════════════════════════════════════════════════
class Trade:
    """
    A complete straddle trade = CE leg + PE leg.
    Either leg can exit independently.
    The trade is closed when both legs are closed.
    """

    def __init__(self, trade_id: int, entry_spot: float):
        self.trade_id    = trade_id
        self.entry_spot  = entry_spot
        self.entry_time  = datetime.now(Config.TZ)
        self.legs: list[Leg] = []

    def add_leg(self, leg: Leg):
        self.legs.append(leg)

    @property
    def is_open(self) -> bool:
        return any(l.is_open for l in self.legs)

    @property
    def total_pnl(self) -> float:
        return sum(l.realised_pnl for l in self.legs)

    def summary(self) -> str:
        lines = [f"Trade #{self.trade_id} | Entry spot: ₹{self.entry_spot:.2f}"]
        for leg in self.legs:
            status = "OPEN" if leg.is_open else f"CLOSED ({leg.exit_reason})"
            lines.append(
                f"  {leg.option_type} {leg.strike} | "
                f"Entry ₹{leg.entry_price:.2f} | "
                f"Exit ₹{leg.exit_price or 'N/A'} | "
                f"P&L ₹{leg.realised_pnl:.2f} | {status}"
            )
        lines.append(f"  Total P&L: ₹{self.total_pnl:.2f}")
        return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════
#  8. BOT — main orchestrator
# ═══════════════════════════════════════════════════════════════

# Input your platform details

# ═══════════════════════════════════════════════════════════════
#  9. ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    cfg = Config()

    # ── Quick validation ─────────────────────────────────────
    missing = [
        k for k in ("API_KEY", "CLIENT_ID", "PASSWORD", "TOTP_SECRET")
        if getattr(cfg, k).startswith("YOUR_")
    ]
    if missing and not cfg.DRY_RUN:
        log.critical(
            "Missing credentials: %s\n"
            "Set them as environment variables or edit Config above.",
            missing,
        )
        sys.exit(1)

    if cfg.DRY_RUN:
        log.warning(
            "⚠️  DRY RUN mode — no real orders will be placed.\n"
            "   Set Config.DRY_RUN = False for live trading."
        )

    bot = StraddleBot(cfg)
    try:
        bot.start()
    except KeyboardInterrupt:
        log.info("Interrupted by user. Stopping bot …")
        bot.stop()
