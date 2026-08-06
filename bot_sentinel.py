# ============================================================
#  SENTINEL v6.1 — LAZY COIN SHORT HUNTER (FIXED)
#
#  Chiến lược: Tìm coin CHƯA TĂNG dù BTC đã tăng,
#              short khi BTC có dấu hiệu đảo chiều.
#
#  FIX so với v6:
#  ✅ FIX 1: Loại coin đã dump quá sâu (SAGA kiểu)
#  ✅ FIX 2: Phân biệt BB nén trước pump vs nén sau dump
#  ✅ FIX 3: Nhãn rủi ro SAFE / CAUTION / HIGH-RISK
#  ✅ FIX 4: Lọc coin sideway quá lâu không có xu hướng rõ
#  ✅ FIX 5: Cải thiện BTC Weakness Detector
#
#  OUTPUT: Chỉ 2 tin Telegram — bảng tổng hợp, không chi tiết từng coin
#
#  Cài đặt: pip install ccxt pandas numpy requests
#  Chạy:    python sentinel_v6_1.py
# ============================================================

import ccxt
import pandas as pd
import numpy as np
import requests
import time
from datetime import datetime

# ─── TELEGRAM ────────────────────────────────────────────────

# ============================================================
# [DEPLOY PATCH] CAU HINH TU BIEN MOI TRUONG - KHONG HARDCODE TOKEN
# ============================================================
import os as _os
import sys as _sys


def _env(name, default=None, required=False):
    v = _os.environ.get(name, default)
    if required and not v:
        print(f"[FATAL] Thieu bien moi truong {name}. "
              f"Tao GitHub Secret / Environment Variable ten {name}.")
        _sys.exit(1)
    return v


def _build_exchange(default_type="spot"):
    """Tao exchange tu bien EXCHANGE_ID. Ho tro proxy + mirror du lieu Binance."""
    import ccxt as _ccxt
    ex_id = _os.environ.get("EXCHANGE_ID", "binance").strip().lower()
    cfg = {"enableRateLimit": True, "options": {"defaultType": default_type}}

    proxy = _os.environ.get("HTTPS_PROXY") or _os.environ.get("HTTP_PROXY")
    if proxy:
        cfg["proxies"] = {"http": proxy, "https": proxy}
        print(f"[EX] Dung proxy: {proxy.split('@')[-1]}")

    ex = getattr(_ccxt, ex_id)(cfg)

    # Mirror du lieu cong khai cua Binance - dung khi bi loi 451 (geo-block)
    if ex_id == "binance" and _os.environ.get("BINANCE_DATA_MIRROR", "0") == "1":
        try:
            ex.urls["api"]["public"] = "https://data-api.binance.vision/api/v3"
            print("[EX] Da bat mirror data-api.binance.vision")
        except Exception as e:
            print(f"[EX] Khong bat duoc mirror: {e}")

    print(f"[EX] San dang dung: {ex_id}")
    return ex

TELEGRAM_TOKEN = _env("TELEGRAM_TOKEN", required=True)
CHAT_ID        = _env("TELEGRAM_CHAT_ID", required=True)

# ─── SCAN CONFIG ─────────────────────────────────────────────
LIMIT_SCAN    = 300        # Số coin spot quét mỗi lần
MIN_VOL_24H   = 300_000    # Volume 24h tối thiểu (USDT)
SCAN_INTERVAL = 3600       # Quét lại mỗi 60 phút (giây)

# ─── BTC BENCHMARK ───────────────────────────────────────────
BTC_LOOKBACK_DAYS = 14
BTC_MIN_PUMP      = 3.0

# ─── LAZY COIN — TIÊU CHÍ CHÍNH ─────────────────────────────
LAZY_MAX_GAIN  = 30.0
LAZY_RS_MAX    = 0.50

# Spike filter
SPIKE_FILTER_PCT = 35.0
SPIKE_LOOKBACK   = 30

# ─── [FIX 1] LOẠI COIN ĐÃ DUMP QUÁ SÂU ──────────────────────
MAX_DROP_FROM_30D_HIGH = -40.0
MAX_DROP_FROM_90D_HIGH = -60.0
WARN_DROP_FROM_30D     = -30.0

# ─── [FIX 2] BB NÉN PATTERN ──────────────────────────────────
BB_SQUEEZE_WIDTH_MAX = 0.12
BB_TREND_LOOKBACK    = 15
BB_UPTREND_BLOCK_PCT = 8.0

# ─── [FIX 4] SIDEWAY ─────────────────────────────────────────
SIDEWAY_MAX_RANGE_PCT = 15.0
SIDEWAY_MIN_LH        = 1

# Cấu trúc giá
LH_LOOKBACK   = 20
MA_FAST       = 20
MA_SLOW       = 50
MA_SLOPE_BARS = 5
MA_SLOPE_MAX  = 0.5

RSI_MAX   = 58
MIN_SCORE = 45

# ─── [FIX 5] BTC WEAKNESS ────────────────────────────────────
BTC_RSI_TURN          = 60
BTC_RSI_PERIOD        = 14
BTC_VOL_BEAR_MULT     = 1.20
BTC_CONSEC_RED        = 2
BTC_FAIL_BREAKOUT_PCT = 1.0
BTC_MIN_WARNINGS      = 2

# ─── SL/TP ───────────────────────────────────────────────────
SL_ATR_MULT  = 1.5
TP1_ATR_MULT = 2.0
TP2_ATR_MULT = 3.5

# ─── EXCHANGE ────────────────────────────────────────────────
exchange = _build_exchange()


# ════════════════════════════════════════════════════════════
#  TELEGRAM
# ════════════════════════════════════════════════════════════
def send_msg(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        _r = requests.post(url, json={
            'chat_id':    CHAT_ID,
            'text':       text,
            'parse_mode': 'Markdown',
        }, timeout=15)
        if _r.status_code != 200:
            print(f"  [TELEGRAM LOI HTTP {_r.status_code}] {_r.text[:300]}")
    except Exception as e:
        print(f"  [TG lỗi] {e}")


# ════════════════════════════════════════════════════════════
#  SPOT FILTER
# ════════════════════════════════════════════════════════════
def get_spot_usdt_pairs(markets: dict, limit: int = None) -> list:
    EXCLUDE = {
        'USDC','BUSD','TUSD','USDP','FDUSD','DAI','FRAX',
        'USDD','GUSD','PAXG','WBTC','WETH','WBNB','STETH',
        'BTC','ETH','BNB',
    }
    result = []
    for symbol, m in markets.items():
        if m.get('type')   != 'spot': continue
        if m.get('spot')   != True:   continue
        if m.get('swap')   == True:   continue
        if m.get('future') == True:   continue
        if m.get('active') != True:   continue
        if m.get('quote')  != 'USDT': continue
        if ':' in symbol:             continue
        base = m.get('base', '').upper()
        if base in EXCLUDE: continue
        if any(base.endswith(x) for x in ['3L','3S','2L','2S','UP','DOWN']): continue
        result.append(symbol)
    return result[:limit] if limit else result


# ════════════════════════════════════════════════════════════
#  INDICATORS
# ════════════════════════════════════════════════════════════
def calc_rsi(series: pd.Series, period: int = 14) -> float:
    if len(series) < period + 1:
        return 50.0
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    rsi   = 100 - (100 / (1 + rs))
    v     = rsi.iloc[-1]
    return float(v) if pd.notna(v) else 50.0


def calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    h, l, c = df['h'], df['l'], df['c']
    prev_c  = c.shift(1)
    tr      = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr     = tr.rolling(period).mean()
    v       = atr.iloc[-1]
    return float(v) if pd.notna(v) else float((h - l).mean())


def ma_slope_pct(series: pd.Series, window: int, bars: int = 5) -> float:
    ma   = series.rolling(window).mean()
    vals = ma.dropna()
    if len(vals) < bars + 1:
        return 0.0
    start = float(vals.iloc[-(bars + 1)])
    end   = float(vals.iloc[-1])
    return (end / start - 1) * 100 if start > 0 else 0.0


def count_lower_highs(highs: pd.Series, lookback: int = 20) -> int:
    h = highs.tail(lookback).values
    swing_highs = []
    for i in range(2, len(h) - 2):
        if h[i] > h[i-1] and h[i] > h[i-2] and h[i] > h[i+1] and h[i] > h[i+2]:
            swing_highs.append(h[i])
    if len(swing_highs) < 2:
        return 0
    return sum(1 for i in range(1, len(swing_highs)) if swing_highs[i] < swing_highs[i-1])


def up_candle_vol_ratio(df4h: pd.DataFrame, lookback: int = 20) -> float:
    recent        = df4h.tail(lookback)
    green         = recent[recent['c'] > recent['o']]
    avg_vol       = float(recent['v'].mean())
    avg_green_vol = float(green['v'].mean()) if len(green) > 2 else avg_vol
    return avg_green_vol / avg_vol if avg_vol > 0 else 1.0


def bb_width_now(c4h: pd.Series, window: int = 20) -> float:
    ma    = c4h.rolling(window).mean()
    std   = c4h.rolling(window).std()
    upper = ma + std * 2
    lower = ma - std * 2
    ma_v  = float(ma.iloc[-1])
    if ma_v <= 0 or pd.isna(ma_v):
        return 0.1
    return float((upper.iloc[-1] - lower.iloc[-1]) / ma_v)


# ════════════════════════════════════════════════════════════
#  [FIX 1] DUMP DEPTH
# ════════════════════════════════════════════════════════════
def check_dump_depth(df1d: pd.DataFrame) -> dict:
    c    = df1d['c']
    h    = df1d['h']
    curr = float(c.iloc[-1])

    lookback_30 = min(30, len(h))
    high_30d    = float(h.tail(lookback_30).max())
    drop_30d    = (curr / high_30d - 1) * 100

    lookback_90 = min(90, len(h))
    high_90d    = float(h.tail(lookback_90).max())
    drop_90d    = (curr / high_90d - 1) * 100

    if   drop_90d < MAX_DROP_FROM_90D_HIGH: risk = 'blocked'
    elif drop_30d < MAX_DROP_FROM_30D_HIGH: risk = 'high_risk'
    elif drop_30d < WARN_DROP_FROM_30D:     risk = 'caution'
    else:                                   risk = 'safe'

    return {
        'risk':     risk,
        'drop_30d': drop_30d,
        'drop_90d': drop_90d,
        'high_30d': high_30d,
        'high_90d': high_90d,
    }


# ════════════════════════════════════════════════════════════
#  [FIX 2] BB SQUEEZE PATTERN
# ════════════════════════════════════════════════════════════
def analyze_bb_squeeze(c4h: pd.Series) -> dict:
    if len(c4h) < BB_TREND_LOOKBACK + 20:
        return {'pattern': 'unknown', 'pre_trend_pct': 0.0, 'is_squeezed': False}

    bw          = bb_width_now(c4h, 20)
    is_squeezed = bw < BB_SQUEEZE_WIDTH_MAX

    if not is_squeezed:
        return {'pattern': 'no_squeeze', 'pre_trend_pct': 0.0, 'is_squeezed': False}

    price_now  = float(c4h.iloc[-1])
    price_prev = float(c4h.iloc[-(BB_TREND_LOOKBACK + 1)])
    pre_trend  = (price_now / price_prev - 1) * 100 if price_prev > 0 else 0.0

    if   pre_trend >= BB_UPTREND_BLOCK_PCT:  pattern = 'bullish_squeeze'
    elif pre_trend <= -BB_UPTREND_BLOCK_PCT: pattern = 'bearish_squeeze'
    else:                                    pattern = 'neutral_squeeze'

    return {
        'pattern':       pattern,
        'pre_trend_pct': pre_trend,
        'is_squeezed':   True,
        'bb_width':      bw,
    }


# ════════════════════════════════════════════════════════════
#  [FIX 4] SIDEWAY
# ════════════════════════════════════════════════════════════
def analyze_sideway(df4h: pd.DataFrame, lookback: int = 30) -> dict:
    recent    = df4h.tail(lookback)
    h_max     = float(recent['h'].max())
    l_min     = float(recent['l'].min())
    if l_min <= 0:
        return {'is_sideway': False, 'range_pct': 0.0, 'has_direction': True}

    range_pct     = (h_max / l_min - 1) * 100
    is_sideway    = range_pct < SIDEWAY_MAX_RANGE_PCT
    lh            = count_lower_highs(df4h['h'], lookback)
    has_direction = lh >= SIDEWAY_MIN_LH

    return {
        'is_sideway':    is_sideway,
        'range_pct':     range_pct,
        'has_direction': has_direction,
        'lower_highs':   lh,
    }


# ════════════════════════════════════════════════════════════
#  SPIKE FILTER
# ════════════════════════════════════════════════════════════
def has_spike(df4h: pd.DataFrame, lookback: int = 30, threshold: float = 35.0) -> bool:
    recent = df4h.tail(lookback)
    for _, row in recent.iterrows():
        o_val = float(row['o'])
        if o_val <= 0:
            continue
        if (float(row['h']) - o_val) / o_val * 100 > threshold:
            return True
    return False


# ════════════════════════════════════════════════════════════
#  [FIX 3] RISK LABEL
# ════════════════════════════════════════════════════════════
def _calc_risk_label(dump_info, bb_info, sw_info, lh, rsi, coin_gain) -> str:
    risk_score = 0

    if   dump_info['risk'] == 'high_risk': risk_score += 3
    elif dump_info['risk'] == 'caution':   risk_score += 1

    if   bb_info['pattern'] == 'bullish_squeeze': risk_score += 2
    elif bb_info['pattern'] == 'neutral_squeeze':  risk_score += 1

    if sw_info['is_sideway'] and not sw_info['has_direction']:
        risk_score += 2

    if rsi < 30:
        risk_score += 2

    if coin_gain < -30:
        risk_score += 1

    if   risk_score >= 4: return 'HIGH-RISK'
    elif risk_score >= 2: return 'CAUTION'
    else:                 return 'SAFE'


# ════════════════════════════════════════════════════════════
#  LAZY COIN CHECK
# ════════════════════════════════════════════════════════════
def check_lazy_coin(df1d: pd.DataFrame, df4h: pd.DataFrame,
                    btc_gain_pct: float) -> dict | None:
    if len(df1d) < BTC_LOOKBACK_DAYS + 2 or len(df4h) < 55:
        return None

    c1d  = df1d['c']
    c4h  = df4h['c']
    curr = float(c4h.iloc[-1])

    coin_start    = float(c1d.iloc[-(BTC_LOOKBACK_DAYS + 1)])
    coin_now      = float(c1d.iloc[-1])
    if coin_start <= 0:
        return None

    coin_gain_pct = (coin_now / coin_start - 1) * 100
    rs_ratio      = coin_gain_pct / btc_gain_pct if btc_gain_pct > 0 else 1.0

    if coin_gain_pct > LAZY_MAX_GAIN: return None
    if rs_ratio      > LAZY_RS_MAX:   return None

    if has_spike(df4h, SPIKE_LOOKBACK, SPIKE_FILTER_PCT):
        return None

    dump_info = check_dump_depth(df1d)
    if dump_info['risk'] == 'blocked':
        return None

    bb_info  = analyze_bb_squeeze(c4h)
    sw_info  = analyze_sideway(df4h, 30)
    lh       = count_lower_highs(df4h['h'], LH_LOOKBACK)
    up_vol_r = up_candle_vol_ratio(df4h, 20)

    ma20_slope = ma_slope_pct(c4h, MA_FAST, MA_SLOPE_BARS)
    ma50_slope = ma_slope_pct(c4h, MA_SLOW, MA_SLOPE_BARS)
    ma20v      = float(c4h.rolling(MA_FAST).mean().iloc[-1])
    ma50v      = float(c4h.rolling(MA_SLOW).mean().iloc[-1])

    if ma20_slope > MA_SLOPE_MAX * 2:
        return None

    rsi_4h = calc_rsi(c4h, 14)
    if rsi_4h > RSI_MAX:
        return None

    bw         = bb_width_now(c4h, 20)
    risk_label = _calc_risk_label(dump_info, bb_info, sw_info, lh, rsi_4h, coin_gain_pct)

    return {
        'coin_gain_pct': coin_gain_pct,
        'rs_ratio':      rs_ratio,
        'lh':            lh,
        'up_vol_r':      up_vol_r,
        'ma20_slope':    ma20_slope,
        'ma50_slope':    ma50_slope,
        'ma20':          ma20v,
        'ma50':          ma50v,
        'rsi_4h':        rsi_4h,
        'bb_width':      bw,
        'curr':          curr,
        'risk_label':    risk_label,
        'dump_info':     dump_info,
        'bb_pattern':    bb_info['pattern'],
        'is_sideway':    sw_info['is_sideway'],
        'sw_range_pct':  sw_info['range_pct'],
    }


# ════════════════════════════════════════════════════════════
#  SCORING
# ════════════════════════════════════════════════════════════
def score_lazy_coin(info: dict, btc_gain_pct: float) -> int:
    s   = 0
    cg  = info['coin_gain_pct']
    rs  = info['rs_ratio']
    lh  = info['lh']
    uvr = info['up_vol_r']
    m20 = info['ma20_slope']
    rsi = info['rsi_4h']
    bw  = info['bb_width']

    if   rs <= 0.0:  s += 35
    elif rs <= 0.10: s += 30
    elif rs <= 0.25: s += 22
    elif rs <= 0.40: s += 14
    elif rs <= 0.50: s +=  7

    if   cg <= -10: s += 15
    elif cg <=   0: s += 12
    elif cg <=  10: s +=  8
    elif cg <=  20: s +=  4

    if   lh >= 3: s += 15
    elif lh >= 2: s += 10
    elif lh >= 1: s +=  5

    if   uvr <= 0.50: s += 20
    elif uvr <= 0.65: s += 14
    elif uvr <= 0.75: s +=  8
    elif uvr <= 0.85: s +=  4

    if   m20 <= -0.5: s += 10
    elif m20 <=  0.0: s +=  7
    elif m20 <=  0.3: s +=  3

    if   rsi <= 40: s += 5
    elif rsi <= 50: s += 3

    if bw <= 0.08:
        s += 3

    risk = info.get('risk_label', 'SAFE')
    if   risk == 'HIGH-RISK': s -= 10
    elif risk == 'CAUTION':   s -=  5

    return max(0, min(100, s))


# ════════════════════════════════════════════════════════════
#  BTC WEAKNESS DETECTOR
# ════════════════════════════════════════════════════════════
def analyze_btc_weakness(df4h_btc: pd.DataFrame) -> dict:
    if len(df4h_btc) < 30:
        return {'level': 0, 'reasons': [], 'rsi': 50.0}

    c = df4h_btc['c']
    o = df4h_btc['o']
    h = df4h_btc['h']
    v = df4h_btc['v']

    reasons    = []
    warnings   = 0
    curr_price = float(c.iloc[-1])

    # 1. RSI quay đầu
    rsi_now   = calc_rsi(c, BTC_RSI_PERIOD)
    rsi_prev3 = calc_rsi(c.iloc[:-3], BTC_RSI_PERIOD)
    rsi_prev6 = calc_rsi(c.iloc[:-6], BTC_RSI_PERIOD)
    rsi_peak  = max(rsi_now, rsi_prev3, rsi_prev6)
    rsi_drop  = rsi_peak - rsi_now

    if rsi_now < BTC_RSI_TURN and rsi_prev3 >= BTC_RSI_TURN:
        warnings += 2
        reasons.append(f"⚡ RSI H4 xuyên xuống {BTC_RSI_TURN} ({rsi_prev3:.0f}→{rsi_now:.0f})")
    elif rsi_drop >= 8 and rsi_peak >= 62:
        warnings += 1
        reasons.append(f"📉 RSI H4 quay đầu: {rsi_peak:.0f}→{rsi_now:.0f} (-{rsi_drop:.0f}đ)")

    # 2. Nến đỏ liên tiếp
    reds = 0
    for i in range(1, 6):
        if float(c.iloc[-i]) < float(o.iloc[-i]):
            reds += 1
        else:
            break
    if reds >= BTC_CONSEC_RED:
        warnings += reds - 1
        reasons.append(f"🕯 {reds} nến đỏ H4 liên tiếp")

    # 3. Nến đỏ to + vol cao
    last       = df4h_btc.iloc[-1]
    last_red   = float(last['c']) < float(last['o'])
    last_body  = abs(float(last['c']) - float(last['o']))
    last_range = float(last['h']) - float(last['l'])
    body_ratio = last_body / last_range if last_range > 0 else 0
    avg_vol    = float(v.tail(20).mean())
    last_vol   = float(last['v'])

    if last_red and body_ratio >= 0.60 and last_vol >= avg_vol * BTC_VOL_BEAR_MULT:
        warnings += 2
        reasons.append(f"🐻 Nến đỏ to (body {body_ratio*100:.0f}%) + Vol {last_vol/avg_vol:.1f}x TB")

    # 4. Phá SMA20
    ma20          = c.rolling(20).mean()
    ma20_now      = float(ma20.iloc[-1])
    ma20_prev     = float(ma20.iloc[-2])
    price_crossed = (float(c.iloc[-2]) >= ma20_prev) and (curr_price < ma20_now)
    price_below   = curr_price < ma20_now

    if price_crossed:
        warnings += 2
        reasons.append(f"🔻 BTC vừa phá xuống SMA20 ({ma20_now:,.0f})")
    elif price_below:
        warnings += 1
        reasons.append(f"⬇️ BTC đang dưới SMA20 ({ma20_now:,.0f})")

    # 5. Bearish divergence
    vol_recent   = float(v.tail(5).mean())
    vol_before   = float(v.iloc[-10:-5].mean())
    price_recent = float(c.tail(5).mean())
    price_before = float(c.iloc[-10:-5].mean())

    if price_recent > price_before and vol_recent < vol_before * 0.75:
        warnings += 1
        reasons.append(f"⚠️ Divergence: Giá tăng nhưng vol giảm {(1-vol_recent/vol_before)*100:.0f}%")

    # 6. Giảm từ đỉnh gần nhất
    recent_high    = float(h.tail(12).max())
    drop_from_high = (curr_price / recent_high - 1) * 100
    if drop_from_high <= -3.0:
        warnings += 1
        reasons.append(f"📊 BTC giảm {drop_from_high:.1f}% từ đỉnh {recent_high:,.0f}")

    # 7. Fail Breakout
    peak_24 = float(h.iloc[-24:-12].max())
    if peak_24 > 0 and curr_price < peak_24 * (1 - BTC_FAIL_BREAKOUT_PCT / 100):
        if rsi_now > 55:
            warnings += 1
            reasons.append(f"🚫 Fail breakout: Không vượt đỉnh {peak_24:,.0f}")

    if   warnings >= 5: level = 3
    elif warnings >= 3: level = 2
    elif warnings >= BTC_MIN_WARNINGS: level = 1
    else:               level = 0

    return {
        'level':          level,
        'warnings':       warnings,
        'reasons':        reasons,
        'rsi':            rsi_now,
        'curr_price':     curr_price,
        'ma20':           ma20_now,
        'drop_from_high': drop_from_high,
        'reds':           reds,
    }


# ════════════════════════════════════════════════════════════
#  MARKET HEALTH
# ════════════════════════════════════════════════════════════
def market_health(red_r: float, fake_r: float, grn_r: float) -> tuple:
    sc  = max(0, min(100, int(50 - red_r * 1.2 - fake_r * 0.8 + grn_r * 0.5)))
    bar = "█" * int(sc / 5) + "░" * (20 - int(sc / 5))
    if   sc >= 65: lb = "🟢 TĂNG TRƯỞNG"; tip = "Dòng tiền khỏe."
    elif sc >= 42: lb = "🟡 SIDEWAY";      tip = "Chờ xác nhận hướng."
    elif sc >= 25: lb = "🟠 CẢNH BÁO";    tip = "Phe bán chiếm ưu thế."
    else:          lb = "🔴 NGUY HIỂM";   tip = "Sập mạnh! Bảo toàn vốn."
    return sc, bar, lb, tip


# ════════════════════════════════════════════════════════════
#  FORMAT TELEGRAM
# ════════════════════════════════════════════════════════════
RISK_EMOJI = {'SAFE': '✅', 'CAUTION': '⚠️', 'HIGH-RISK': '🚨'}
RISK_SHORT = {'SAFE': 'OK', 'CAUTION': 'CAUT', 'HIGH-RISK': 'HIGH'}


def fmt_msg1(now_str, btc_gain, total,
             red_r, grn_r, fak_r,
             sc, bar, lb, tip,
             lazy_coins,
             safe_count, caut_count, high_count) -> str:
    """
    Tin 1: Market Health + bảng Lazy Coins (top 10).
    """
    lines = [
        f"🦅 *SENTINEL v6.1 Claude tìm lệnh short — {now_str}*",
        f"BTC {BTC_LOOKBACK_DAYS}D: `{btc_gain:+.1f}%`",
        f"Spot: `{total}` cặp",
        "─────────────────────",
        f"🔴 Đỏ mạnh:   `{red_r:.1f}%`",
        f"🟢 Xanh thật: `{grn_r:.1f}%`",
        f"⚠️ Hồi lừa:   `{fak_r:.1f}%`",
        "─────────────────────",
        f"Score: `{sc}/100`  `[{bar}]`",
        f"{lb} — _{tip}_",
    ]

    if lazy_coins:
        lines.append("─────────────────────")
        lines.append(
            f"😴 *LAZY COINS — Chưa tăng dù BTC {btc_gain:+.1f}%*\n"
            f"✅SAFE={safe_count} | ⚠️CAUTION={caut_count} | 🚨HIGH-RISK={high_count}"
        )
        table = ["```", "Coin     RS%  Gain  UpVol  Risk  Sc", "─" * 36]
        for c in lazy_coins[:10]:
            coin       = c['symbol'].split('/')[0][:7].ljust(7)
            risk_short = RISK_SHORT.get(c['risk_label'], '?')
            table.append(
                f"{coin} "
                f"{c['rs_ratio']*100:4.0f}%  "
                f"{c['coin_gain']:+4.0f}%  "
                f"{c['up_vol_r']:4.2f}  "
                f"{risk_short:4s}  "
                f"{c['score']:2d}"
            )
        table.append("```")
        lines.append("\n".join(table))
    else:
        lines.append("─────────────────────")
        lines.append(
            f"😴 *Lazy Coins:* 0 coin lười tìm được.\n"
            f"_(Altcoin đang theo kịp hoặc BTC chưa tăng đủ)_"
        )

    return "\n".join(lines)


def fmt_msg2(btc: dict, lazy_coins: list, now_str: str) -> str:
    """
    Tin 2: BTC Weakness + danh sách coin gọn có SL/TP.
    """
    level_emoji = {3: "🔴", 2: "🟠", 1: "🟡"}.get(btc['level'], "✅")
    level_text  = {
        3: "NGUY HIỂM — SHORT NGAY",
        2: "CẢNH BÁO — CHUẨN BỊ SHORT",
        1: "CHÚ Ý — THEO DÕI",
    }.get(btc['level'], "BTC ổn định")

    lines = [
        f"{level_emoji} *BTC WEAKNESS — {now_str}*",
        f"Mức: *{level_text}*",
        f"BTC: `{btc['curr_price']:,.0f}` | RSI: `{btc['rsi']:.0f}` | SMA20: `{btc['ma20']:,.0f}`",
        f"Từ đỉnh gần: `{btc['drop_from_high']:.1f}%`",
        "─────────────────────",
        "*Tín hiệu yếu:*",
    ]
    for r in btc['reasons']:
        lines.append(f"  {r}")

    if lazy_coins:
        # SAFE + CAUTION: hiển thị đầy đủ SL/TP
        tradeable = [c for c in lazy_coins if c['risk_label'] in ('SAFE', 'CAUTION')]
        if tradeable:
            lines.append("─────────────────────")
            lines.append(f"🎯 *{len(tradeable)} LAZY COIN (SAFE/CAUTION):*")
            for c in tradeable[:10]:
                coin  = c['symbol'].split('/')[0]
                emoji = RISK_EMOJI.get(c['risk_label'], '❓')
                lines.append(
                    f"{emoji} *{coin}* `{c['curr']:.6f}` "
                    f"RS:`{c['rs_ratio']*100:.0f}%` Sc:`{c['score']}`"
                )
                lines.append(
                    f"  🔴`{c['sl']:.6f}` "
                    f"✅`{c['tp1']:.6f}` "
                    f"✅`{c['tp2']:.6f}`"
                )

        # HIGH-RISK: chỉ cảnh báo
        hr_list = [c for c in lazy_coins if c['risk_label'] == 'HIGH-RISK']
        if hr_list:
            lines.append("─────────────────────")
            lines.append(f"🚨 *{len(hr_list)} coin HIGH-RISK (KHÔNG NÊN SHORT):*")
            for c in hr_list[:5]:
                coin = c['symbol'].split('/')[0]
                lines.append(
                    f"  • {coin} — đã giảm {c['dump_info']['drop_30d']:.0f}% từ đỉnh"
                )

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
#  FETCH BTC
# ════════════════════════════════════════════════════════════
def fetch_btc_data() -> tuple:
    try:
        raw1d = exchange.fetch_ohlcv('BTC/USDT', '1d', limit=BTC_LOOKBACK_DAYS + 5)
        df1d  = pd.DataFrame(raw1d, columns=['t','o','h','l','c','v'])
        df1d[['o','h','l','c','v']] = df1d[['o','h','l','c','v']].apply(pd.to_numeric)

        raw4h = exchange.fetch_ohlcv('BTC/USDT', '4h', limit=60)
        df4h  = pd.DataFrame(raw4h, columns=['t','o','h','l','c','v'])
        df4h[['o','h','l','c','v']] = df4h[['o','h','l','c','v']].apply(pd.to_numeric)

        btc_start = float(df1d['c'].iloc[-(BTC_LOOKBACK_DAYS + 1)])
        btc_now   = float(df1d['c'].iloc[-1])
        btc_gain  = (btc_now / btc_start - 1) * 100
        return df1d, df4h, btc_gain
    except Exception as e:
        print(f"  [BTC fetch lỗi] {e}")
        return pd.DataFrame(), pd.DataFrame(), 0.0


# ════════════════════════════════════════════════════════════
#  MAIN SCAN
# ════════════════════════════════════════════════════════════
_STATE_FILE = _os.environ.get("STATE_FILE", "sentinel_state.json")


def _load_state():
    import json
    try:
        with open(_STATE_FILE, encoding="utf-8") as f:
            return int(json.load(f).get("prev_btc_level", 0))
    except Exception:
        return 0


def _save_state(level):
    import json
    try:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"prev_btc_level": int(level)}, f)
    except Exception as e:
        print(f"[state] khong luu duoc: {e}")


_prev_btc_level = _load_state()


def run_sentinel():
    global _prev_btc_level

    now_str = datetime.now().strftime('%H:%M %d/%m/%Y')
    print(f"\n{'='*56}")
    print(f"  SENTINEL v6.1 — {now_str}")
    print(f"{'='*56}")

    try:
        markets = exchange.load_markets()
    except Exception as e:
        print(f"  Lỗi load markets: {e}"); return

    spot_pairs = get_spot_usdt_pairs(markets, limit=LIMIT_SCAN)
    print(f"  Spot USDT hợp lệ: {len(spot_pairs)} cặp")

    df1d_btc, df4h_btc, btc_gain = fetch_btc_data()
    if df1d_btc.empty:
        print("  Không lấy được BTC data."); return
    print(f"  BTC gain {BTC_LOOKBACK_DAYS}D: {btc_gain:+.1f}%")

    btc_weakness = {}
    btc_level    = 0
    if not df4h_btc.empty:
        btc_weakness = analyze_btc_weakness(df4h_btc)
        btc_level    = btc_weakness.get('level', 0)
        print(f"  BTC Weakness: Level {btc_level}/3 "
              f"| RSI: {btc_weakness.get('rsi',0):.0f} "
              f"| Warnings: {btc_weakness.get('warnings',0)}")

    red_cnt = green_cnt = fake_cnt = total = 0
    lazy_coins = []
    skip_stats = {}

    def skip(reason):
        skip_stats[reason] = skip_stats.get(reason, 0) + 1

    for symbol in spot_pairs:
        try:
            raw1 = exchange.fetch_ohlcv(symbol, '1d', limit=max(90, BTC_LOOKBACK_DAYS) + 5)
            if len(raw1) < BTC_LOOKBACK_DAYS + 2:
                skip('short_history'); continue

            df1 = pd.DataFrame(raw1, columns=['t','o','h','l','c','v'])
            df1[['o','h','l','c','v']] = df1[['o','h','l','c','v']].apply(pd.to_numeric)

            curr1d = float(df1['c'].iloc[-1])
            chg1d  = (curr1d / float(df1['c'].iloc[-2]) - 1) * 100
            vr1d   = float(df1['v'].iloc[-1]) / float(df1['v'].tail(10).mean())
            total += 1

            if   chg1d < -3:                 red_cnt   += 1
            elif chg1d > 3 and vr1d >= 1.1:  green_cnt += 1
            elif chg1d > 1 and vr1d < 0.8:   fake_cnt  += 1

            try:
                tkr = exchange.fetch_ticker(symbol)
                if (tkr.get('quoteVolume') or 0) < MIN_VOL_24H:
                    skip('vol_too_low'); continue
            except Exception:
                pass

            raw4 = exchange.fetch_ohlcv(symbol, '4h', limit=75)
            if len(raw4) < 55:
                skip('4h_short'); continue

            df4 = pd.DataFrame(raw4, columns=['t','o','h','l','c','v'])
            df4[['o','h','l','c','v']] = df4[['o','h','l','c','v']].apply(pd.to_numeric)

            info = check_lazy_coin(df1, df4, btc_gain)
            if info is None:
                skip('not_lazy'); continue

            score = score_lazy_coin(info, btc_gain)
            if score < MIN_SCORE:
                skip('score_low'); continue

            curr = info['curr']
            atr  = calc_atr(df4, 14)
            sl   = curr + SL_ATR_MULT  * atr
            tp1  = curr - TP1_ATR_MULT * atr
            tp2  = curr - TP2_ATR_MULT * atr
            rr   = (curr - tp1) / max(sl - curr, 1e-10)

            lazy_coins.append({
                'symbol':       symbol,
                'curr':         curr,
                'coin_gain':    info['coin_gain_pct'],
                'rs_ratio':     info['rs_ratio'],
                'lh':           info['lh'],
                'up_vol_r':     info['up_vol_r'],
                'ma20_slope':   info['ma20_slope'],
                'rsi_4h':       info['rsi_4h'],
                'bb_width':     info['bb_width'],
                'ma20':         info['ma20'],
                'risk_label':   info['risk_label'],
                'dump_info':    info['dump_info'],
                'bb_pattern':   info['bb_pattern'],
                'is_sideway':   info['is_sideway'],
                'sw_range_pct': info['sw_range_pct'],
                'atr': atr, 'sl': sl, 'tp1': tp1, 'tp2': tp2,
                'rr': rr, 'score': score,
            })

            time.sleep(0.05)

        except Exception:
            continue

    lazy_coins.sort(key=lambda x: x['score'], reverse=True)

    red_r = red_cnt / total * 100   if total else 0
    grn_r = green_cnt / total * 100 if total else 0
    fak_r = fake_cnt / total * 100  if total else 0
    sc, bar, lb, tip = market_health(red_r, fak_r, grn_r)

    safe_count = sum(1 for c in lazy_coins if c['risk_label'] == 'SAFE')
    caut_count = sum(1 for c in lazy_coins if c['risk_label'] == 'CAUTION')
    high_count = sum(1 for c in lazy_coins if c['risk_label'] == 'HIGH-RISK')

    print(f"\n  [Skip stats]")
    for k, v in sorted(skip_stats.items()):
        print(f"  {k:30s}: {v}")
    print(f"\n  Lazy: SAFE={safe_count} | CAUTION={caut_count} | HIGH-RISK={high_count}")

    # ── Tin 1: Market Health + Lazy Table ──
    send_msg(fmt_msg1(
        now_str, btc_gain, total,
        red_r, grn_r, fak_r,
        sc, bar, lb, tip,
        lazy_coins,
        safe_count, caut_count, high_count,
    ))

    # ── Tin 2: BTC Weakness + Coin List (chỉ khi BTC yếu) ──
    if btc_weakness and btc_level >= 1:
        send_msg(fmt_msg2(btc_weakness, lazy_coins, now_str))
        _prev_btc_level = btc_level
        _save_state(btc_level)
        print(f"  BTC Alert Level {btc_level} đã gửi.")
    elif _prev_btc_level >= 2 and btc_level == 0:
        send_msg(
            f"✅ *BTC HỒI PHỤC — {now_str}*\n"
            f"BTC thoát vùng nguy hiểm.\n"
            f"Nếu đang short lazy coins → cân nhắc chốt lời 1 phần."
        )
        _prev_btc_level = 0
        _save_state(0)

    print(f"\n  Xong. Nghỉ {SCAN_INTERVAL // 60} phút.\n")


# ════════════════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    RUN_ONCE = _os.environ.get("RUN_ONCE", "0") == "1"
    print("=" * 56)
    print("  SENTINEL v6.1 - LAZY COIN SHORT HUNTER")
    print(f"  Che do: {'QUET 1 LAN (CI)' if RUN_ONCE else 'CHAY LIEN TUC'}")
    print("=" * 56)

    # Preflight: neu san chan IP (loi 451) thi FAIL RO, khong "xanh gia"
    if RUN_ONCE:
        try:
            _n = len(exchange.load_markets())
            print(f"  Ket noi san OK - {_n} markets")
        except Exception as _e:
            print(f"[FATAL] Khong ket noi duoc san: {_e}")
            print("[FATAL] Neu la loi 451 -> doi Variable EXCHANGE_ID sang kucoin/okx/bybit/gateio")
            _sys.exit(1)

    if RUN_ONCE:
        run_sentinel()
        print("Xong 1 lan quet, thoat.")
    else:
        while True:
            try:
                run_sentinel()
            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"[LOI VONG QUET] {e} - thu lai sau {SCAN_INTERVAL // 60} phut")
            time.sleep(SCAN_INTERVAL)
