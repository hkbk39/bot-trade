#!/usr/bin/env python3
"""combo.py - tin hieu BUY combo 3of3/4of4/4of5 tu nen NGAY (da dong) + loc universe Spot.

Vendored/adapted tu projects/coin-auto-trader/signals.py (chi phan BUY, bo het phan SELL vi
bot nay CHI co 1 chieu BUY). KHONG sua file goc ben kia. Logic giu dung 1:1 voi
outputs/full2026_buy_combo_bb24h/run_full2026_add_n7.py (ham _buy_signal/_buy_qualified).
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

VN_TZ = timezone(timedelta(hours=7))
HOUR_MS = 3_600_000
DAY_MS = 86_400_000

# Mirror signals.py: coin stable/khong phai crypto/leverage token -> loai khoi universe.
STABLES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "AEUR", "EURI", "EUR", "BUSD", "USTC", "USDD", "SUSD",
           "XUSD", "USD1", "BFUSD", "EURT", "TRY", "BRL"}
NON_CRYPTO = {"TSLAB", "NVDAB", "MSTRB", "QQQB", "INTCB", "SNDKB", "CRCLB", "SPCXB", "SOXLB", "SKHYB",
              "SNXXB", "KORUB", "MUB", "XAUT", "RLUSD"}
LEV_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


def vn_str(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, VN_TZ).strftime("%Y-%m-%d %H:%M")


def iso_day(open_ms: int) -> str:
    return datetime.fromtimestamp(int(open_ms) / 1000, timezone.utc).date().isoformat()


def excluded_base(base: str) -> bool:
    if base in STABLES or base in NON_CRYPTO:
        return True
    return any(base.endswith(x) and len(base) > len(x) for x in LEV_SUFFIXES)


def closed_daily(daily_rows, now_ms: int):
    """Chi nen ngay DA DONG (close_time < now_ms). Cot index 6 = closeTime trong kline Binance."""
    return [r for r in daily_rows if int(r[6]) < int(now_ms)]


def daily_return(row) -> float:
    return float(row[4]) / float(row[1]) - 1.0


def daily_returns(rows):
    return [daily_return(r) for r in rows]


def _buy_qualified(rets, index: int, window: int, need: int, thr: float) -> bool:
    if index < window - 1:
        return False
    return sum(x >= thr for x in rets[index - window + 1:index + 1]) >= need


def _buy_signal(rows, rets, index: int, combo_name: str, need: int, window: int, green_pct: float):
    """Mirror signals.py::_buy_signal - tin hieu CHI o ngay DAU chuoi dat dieu kien."""
    thr = green_pct / 100.0
    if not _buy_qualified(rets, index, window, need, thr):
        return None
    if index - 1 >= window - 1 and _buy_qualified(rets, index - 1, window, need, thr):
        return None  # khong phai ngay dau chuoi
    row = rows[index]
    signal_open_ms = int(row[0])
    return {
        "combo": combo_name,
        "signal_open_ms": signal_open_ms,
        "signal_close_ms": signal_open_ms + DAY_MS,
        "signal_date": iso_day(signal_open_ms),
        "daily_return_pct": round(rets[index] * 100, 6),
    }


def buy_signals_for_symbol(daily_rows, now_ms, combos_order, strong_green_pct=4.0):
    """Quet combo BUY tren NEN NGAY CUOI DA DONG cho 1 symbol (giong scanner live 07:xx VN).

    combos_order: list [{"combo":"4of5","need":4,"window_days":5}, ...] theo DUNG thu tu uu
    tien (combo dau tien trong list thang khi 2+ combo cung khop CUNG 1 ngay tin hieu - dung
    y nguyen thu tu combos trong config.json cua coin-auto-trader, da chot 15/09/2026:
    4of5 -> 3of3 -> 4of4).
    """
    rows = closed_daily(daily_rows, now_ms)
    out = []
    for c in combos_order:
        need, window = int(c["need"]), int(c["window_days"])
        if len(rows) < window:
            continue
        rets = daily_returns(rows)
        sig = _buy_signal(rows, rets, len(rows) - 1, c["combo"], need, window, strong_green_pct)
        if sig:
            out.append(sig)
    if not out:
        return None
    # Dedup: >=2 combo cung khop cung 1 ngay tin hieu (cung signal_open_ms, chac chan vi dung
    # index=len(rows)-1 cho tat ca) -> chi lay 1, uu tien theo DUNG thu tu combos_order.
    order_index = {c["combo"]: i for i, c in enumerate(combos_order)}
    out.sort(key=lambda s: order_index.get(s["combo"], 99))
    return out[0]


def risky_reason(risky_coins, symbol: str):
    if symbol in set(risky_coins):
        return "RISKY_COINS (danh sach tham khao tu coin-auto-trader cu)"
    return None
