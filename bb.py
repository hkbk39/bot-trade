#!/usr/bin/env python3
"""bb.py - Bollinger Band H1 (period 20, sigma 2.0, ddof=0).

Vendored 1:1 tu projects/coin-auto-trader/bb.py (chi giu phan dung cho bot nay:
tinh band + doc cot nen Binance klines). KHONG sua file goc ben kia.
"""
from __future__ import annotations

import math

C_OPEN_MS, C_OPEN, C_HIGH, C_LOW, C_CLOSE, C_CLOSE_MS, C_QUOTE_VOL = 0, 1, 2, 3, 4, 6, 7


def candle_open_ms(candle) -> int:
    return int(candle[C_OPEN_MS])


def candle_open(candle) -> float:
    return float(candle[C_OPEN])


def candle_high(candle) -> float:
    return float(candle[C_HIGH])


def candle_low(candle) -> float:
    return float(candle[C_LOW])


def candle_close(candle) -> float:
    return float(candle[C_CLOSE])


def candle_quote_volume(candle) -> float:
    """Quote volume (USDT) cua nen - cot index 7 trong kline array cua Binance."""
    return float(candle[C_QUOTE_VOL])


def closes(candles) -> list:
    return [float(c[C_CLOSE]) for c in candles]


def bands(closes_list, period: int = 20, sigma: float = 2.0, ddof: int = 0):
    """list[(upper, mid, lower) | None] - mirror y nguyen bb.py goc (running sum, ddof=0)."""
    out = [None] * len(closes_list)
    if period <= 0 or len(closes_list) < period:
        return out
    s = 0.0
    sq = 0.0
    denom = float(period - ddof) if period - ddof > 0 else float(period)
    for i, c in enumerate(closes_list):
        s += c
        sq += c * c
        if i >= period:
            old = closes_list[i - period]
            s -= old
            sq -= old * old
        if i >= period - 1:
            mean = s / period
            var = sq / period - mean * mean if ddof == 0 else (sq - s * s / period) / denom
            sd = math.sqrt(max(0.0, var))
            out[i] = (mean + sigma * sd, mean, mean - sigma * sd)
    return out


def bands_for(candles, period: int = 20, sigma: float = 2.0, ddof: int = 0):
    return bands(closes(candles), period=period, sigma=sigma, ddof=ddof)
