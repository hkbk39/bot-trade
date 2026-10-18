import ccxt
import pandas as pd
import requests
import re
import time
import matplotlib
matplotlib.use("Agg")   # chay khong can man hinh (CI/server)
import mplfinance as mpf
import os
import threading
from flask import Flask, jsonify
from datetime import datetime
from prettytable import PrettyTable

# ============================================================
# CẤU HÌNH BIẾN
# ============================================================

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

TOKEN   = _env("TELEGRAM_TOKEN", required=True)
CHAT_ID = _env("TELEGRAM_CHAT_ID", required=True)

SCAN_DAYS        = 16
H4_MS            = 4 * 3600 * 1000
# [PATCH 2026-10-18] Dong bo CHINH XAC voi tham so BEST dang chay LIVE tien thuc
# tren VPS (projects/coin-spot-v74-2slot/v74core.py, chot backtest 2026-10-17
# 21:59: MIN_GROWTH=0.35, MAX_DAYS_PEAK=9.0, SQZ_MAX=0.25). Truoc ban vay la
# 40%/5 ngay/0,30 - lech voi dieu kien bot dang vao lenh that, khien app hien
# tin hieu khac voi bot VPS. Theo yeu cau user 2026-10-18.
MIN_GROWTH_PCT   = 35
MAX_DAYS_FROM_PEAK = float(_os.environ.get("MAX_DAYS_FROM_PEAK", "9"))
MIN_DAYS_FROM_PEAK = float(_os.environ.get("MIN_DAYS_FROM_PEAK", "1.5"))
MIN_PULLBACK_PCT = 30
SQUEEZE_THRESHOLD = 0.25
VOL_EXHAUST_RATIO = 0.9

FLASK_PORT   = int(_os.environ.get("PORT", 5000))   # Render tu cap PORT
# [PATCH 2026-10-16b] Nhip quet khi chay bot_v74 truc tiep (mac dinh 2 gio/lan)
SCAN_INTERVAL = int(_os.environ.get("V74_INTERVAL", 7200))
MAX_HISTORY  = 24   # Lưu tối đa 24 lần quét gần nhất (~24h nếu quét mỗi 1h)

# ============================================================
# BIẾN TOÀN CỤC
# ============================================================
# Mỗi phần tử: { "scan_time": "HH:mm:ss dd/MM/YYYY", "signals": [...] }
scan_history   = []   # <-- MỚI: lưu toàn bộ lịch sử
latest_signals = []
last_scan_time = ""
scan_status    = "Chưa quét"

exchange = _build_exchange()

# [PATCH 2026-10-16] Chan doan: dem so cap quet duoc / so loi de khong con "0 tin hieu" vo hinh
scan_debug     = {}

# ============================================================
# [PATCH 2026-10-16d] NGUON DU LIEU WEBSOCKET — HET BI BAN IP
#
#  V74_DATA_SOURCE:
#    "ws"   -> chi dung WS, khong bao gio goi REST de quet (cho feed san sang)
#    "auto" -> WS neu dung duoc (mac dinh), chi quay ve REST khi WS hong han
#    "rest" -> hanh vi cu (goi REST fetch_ohlcv tung cap)
#  WebSocket KHONG ton request weight => khong the bi 418 "IP banned" vi quet.
# ============================================================
V74_DATA_SOURCE      = _os.environ.get("V74_DATA_SOURCE", "auto").strip().lower()
NO_SIGNAL_NOTIFY_SEC = int(_os.environ.get("NO_SIGNAL_NOTIFY_SEC", "21600"))   # 6h
ALERT_DEDUP_SEC      = int(_os.environ.get("ALERT_DEDUP_SEC", "43200"))        # 12h

ws_feed = None
if V74_DATA_SOURCE in ("auto", "ws"):
    try:
        from ws_feed import BinanceKlineFeed, WS_AVAILABLE as _WS_AVAILABLE
        if not _WS_AVAILABLE:
            print("[WS] Thieu thu vien websocket-client -> dung REST")
        else:
            ws_feed = BinanceKlineFeed(
                interval="4h",
                limit=150,
                rest=exchange,
                backfill_rpm=float(_os.environ.get("WS_BACKFILL_RPM", "60")),
                min_candles=int(_os.environ.get("WS_MIN_CANDLES", "60")),
                stale_sec=float(_os.environ.get("WS_STALE_SEC", "180")),
            )
            print(f"[WS] Da nap module ws_feed — nguon du lieu: {V74_DATA_SOURCE}")
    except Exception as _e:
        print(f"[WS] Khong nap duoc ws_feed: {_e} -> dung REST")

# Tin hieu da ban / moc lan cuoi gui tin "khong co tin hieu"
alerted_at          = {}
last_no_signal_sent = 0.0


def _ws_mode():
    """True neu dang chay o che do WebSocket."""
    return ws_feed is not None and V74_DATA_SOURCE in ("auto", "ws")


def _use_rest_scan():
    """Co duoc phep quet bang REST khong (che do rest, hoac WS hong han)."""
    if not _ws_mode():
        return True
    return ws_feed.failed_hard()


def _alert_allowed(symbol):
    """Chong spam: khong gui lai cung 1 tin hieu trong ALERT_DEDUP_SEC."""
    if time.time() - alerted_at.get(symbol, 0) < ALERT_DEDUP_SEC:
        return False
    alerted_at[symbol] = time.time()
    return True

# ============================================================
# [PATCH 2026-10-16b] CHONG BAN BINANCE
# Binance tra 418/-1003 kem moc "IP banned until <epoch_ms>". Truoc day bot van
# goi lai moi vong -> vi pham lap -> Binance KEO DAI thoi gian ban.
# Gio: doc moc ban, nghi cho het ban roi moi quet lai.
# ============================================================
BAN_UNTIL_MS = 0     # epoch ms; > now = dang bi ban
BAN_REASON   = ""


def _extract_ban_until(text):
    """Tim 'IP banned until <ms>' trong thong bao loi. Tra ve epoch ms hoac None."""
    s = str(text)
    key = "banned until "
    i = s.find(key)
    if i < 0:
        return None
    j = i + len(key)
    digits = ""
    while j < len(s) and s[j].isdigit():
        digits += s[j]
        j += 1
    if not digits:
        return None
    try:
        v = int(digits)
    except Exception:
        return None
    if v < 10 ** 11:      # neu Binance tra ve giay thi doi sang ms
        v *= 1000
    return v


def _note_ban(err_text):
    """Ghi nho moc het ban. Tra ve True neu loi la ban IP."""
    global BAN_UNTIL_MS, BAN_REASON
    ms = _extract_ban_until(err_text)
    if not ms:
        return False
    if ms > BAN_UNTIL_MS:
        BAN_UNTIL_MS = ms
    BAN_REASON = str(err_text)[:200]
    return True


def _ban_left_sec():
    return max(0, int((BAN_UNTIL_MS - time.time() * 1000) / 1000))


def _ban_status_text():
    if _ban_left_sec() <= 0:
        return "San sang quet"
    end = datetime.fromtimestamp(BAN_UNTIL_MS / 1000).strftime('%H:%M:%S %d/%m/%Y')
    return (f"Tam nghi — Binance ban IP toi {end} "
            f"(con {_ban_left_sec() // 60} phut)")

# ============================================================
# [PATCH 2026-10-17] LOC COIN CO THONG BAO DELISTING
# Binance thuong cong bo delist truoc ~10 ngay (catalogId=161 "Delisting").
# Loai cac coin nay khoi universe TRUOC khi quet tin hieu, vi lenh V74 co the
# giu toi 120h (5 ngay) va khong co stoploss cung -> dinh delist giua luc giu
# lenh se rat nguy hiem (mat thanh khoan / bi force-sell).
# ============================================================
DELIST_CACHE_TTL_SEC = 6 * 3600   # lam moi toi da 6h/lan, khong spam API CMS
DELIST_LOOKBACK_DAYS = 30         # chi tinh thong bao cong bo trong 30 ngay gan nhat
_delist_blocklist     = set()
_delist_blocklist_ts  = 0.0

# Chi doc ticker tu TIEU DE dang "Binance Will Delist X, Y on 2026-10-28"
# (da kiem chung day la dang chuan cho cac thong bao neu dich danh ten coin).
# KHONG fetch trang chi tiet: trang nay bi AWS WAF chan (tra 202 "challenge",
# 0 byte) khi goi tu server -> da thu va xac nhan khong doc duoc, bo qua de
# tranh goi API vo ich/bi nghi la bot. Thong bao dang batch khong neu ten coin
# trong title (vd "Notice of Removal of Spot Trading Pairs - <ngay>") se
# KHONG duoc loc tu dong — chi la fallback thieu, khong lam fail ca he thong.
def _extract_tickers_from_title(title):
    """Vd: 'Binance Will Delist EPIC, QUICK on 2026-10-28' -> {'EPIC','QUICK'}."""
    m = re.search(r"[Dd]elist\s+([A-Z0-9]+(?:\s*,\s*[A-Z0-9]+)*)\s+on\b", title or "")
    if not m:
        return set()
    return {t.strip() for t in m.group(1).split(",") if t.strip()}


def get_delisting_blocklist():
    """Tra ve set ticker (BASE, vd 'USDP') dang co thong bao delist con hieu luc
    trong {DELIST_LOOKBACK_DAYS} ngay gan nhat. Fail-open: loi mang/API thi tra
    ve cache cu (hoac rong) — KHONG lam fail vong quet chinh."""
    global _delist_blocklist, _delist_blocklist_ts
    now = time.time()
    if _delist_blocklist_ts and (now - _delist_blocklist_ts) < DELIST_CACHE_TTL_SEC:
        return _delist_blocklist

    try:
        resp = requests.get(
            "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query",
            params={"type": 1, "pageNo": 1, "pageSize": 20, "catalogId": 161},
            timeout=8,
        )
        articles = resp.json()["data"]["catalogs"][0]["articles"]
        cutoff_ms = (now - DELIST_LOOKBACK_DAYS * 86400) * 1000

        new_blocklist = set()
        for art in articles:
            if art.get("releaseDate", 0) < cutoff_ms:
                continue
            new_blocklist |= _extract_tickers_from_title(art.get("title", ""))

        _delist_blocklist = new_blocklist
        _delist_blocklist_ts = now
        if new_blocklist:
            print(f"[DELIST] Blocklist moi ({len(new_blocklist)} coin): {sorted(new_blocklist)}")
    except Exception as e:
        print(f"[DELIST] Loi lam moi blocklist, dung cache cu ({len(_delist_blocklist)} coin): {e}")

    return _delist_blocklist

# ============================================================
# FLASK API SERVER
# ============================================================
flask_app = Flask(__name__)

@flask_app.route('/signals')
def get_signals():
    """Trả về lần quét mới nhất + toàn bộ lịch sử cho Android app."""
    return jsonify({
        "signals":     latest_signals,
        "last_scan":   last_scan_time,
        "status":      scan_status,
        "total_found": len(latest_signals),
        "debug":       scan_debug,          # [PATCH 2026-10-16] so cap quet / so loi
        # MỚI: Android dùng history để bù các lần miss
        "history":     scan_history
    })

@flask_app.route('/ws')
def get_ws():
    """[PATCH 2026-10-16d] Suc khoe nguon du lieu WebSocket (khong ton request weight)."""
    if ws_feed is None:
        return jsonify({"enabled": False, "data_source": V74_DATA_SOURCE})
    st = ws_feed.stats()
    st.update({"enabled": True, "data_source": V74_DATA_SOURCE,
               "usable": ws_feed.is_usable(), "failed_hard": ws_feed.failed_hard()})
    return jsonify(st)


@flask_app.route('/debug')
def get_debug():
    """[PATCH 2026-10-16] Chan doan tu xa: quet duoc bao nhieu cap, loi bao nhieu, mau loi."""
    return jsonify({
        "status":      scan_status,
        "last_scan":   last_scan_time,
        "history_len": len(scan_history),
        "debug":       scan_debug,
    })

@flask_app.route('/health')
def health():
    return jsonify({"ok": True, "time": datetime.now().strftime('%H:%M:%S')})

def run_flask():
    flask_app.run(host='0.0.0.0', port=FLASK_PORT, debug=False, use_reloader=False)

# ============================================================
# GỬI TELEGRAM
# ============================================================
def send_telegram_msg(message):
    url     = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {'chat_id': CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}
    try:
        _r = requests.post(url, json=payload, timeout=10)
        if _r.status_code != 200:
            print(f"[TELEGRAM LOI HTTP {_r.status_code}] {_r.text[:300]}")
    except Exception as e:
        print(f"[Telegram MSG lỗi] {e}")

def send_telegram_photo(message, photo_path):
    url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            payload = {'chat_id': CHAT_ID, 'caption': message, 'parse_mode': 'Markdown'}
            files   = {'photo': photo}
            requests.post(url, data=payload, files=files, timeout=15)
    except Exception as e:
        print(f"[Telegram PHOTO lỗi] {e}")

# ============================================================
# VẼ VÀ LƯU CHART
# ============================================================
def save_v74_chart(symbol, df_raw):
    path     = f"{symbol.replace('/', '_')}.png"
    df_chart = pd.DataFrame(df_raw.values[:, :6],
                            columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df_chart['Date'] = pd.to_datetime(df_chart['Date'], unit='ms')
    df_chart.set_index('Date', inplace=True)
    df_chart = df_chart.apply(pd.to_numeric)

    ma20  = df_chart['Close'].rolling(window=20).mean()
    std   = df_chart['Close'].rolling(window=20).std()
    upper = ma20 + (std * 2)
    lower = ma20 - (std * 2)

    last_60 = df_chart.tail(60)
    ap = [
        mpf.make_addplot(upper.tail(60), color='#FF5252', width=0.8, linestyle='dashed'),
        mpf.make_addplot(lower.tail(60), color='#26A69A', width=0.8, linestyle='dashed'),
        mpf.make_addplot(ma20.tail(60),  color='#2196F3', width=0.6)
    ]

    mpf.plot(last_60, type='candle', style='charles', addplot=ap,
             savefig=path, volume=True, tight_layout=True,
             title=f"\n{symbol} 4H — V74 SIGNAL")
    return path

# ============================================================
# LOGIC QUÉT CHÍNH
# ============================================================
def run_v74_standard():
    global latest_signals, last_scan_time, scan_status, scan_history, scan_debug, BAN_UNTIL_MS
    global last_no_signal_sent

    table = PrettyTable()
    table.field_names = ["Coin", "Up%", "Down%", "Peak", "Sqze", "Vol"]
    table.align = "l"

    scan_time_str = datetime.now().strftime('%H:%M:%S %d/%m/%Y')

    # [PATCH 2026-10-16b] Dang trong thoi gian bi ban -> KHONG goi API, nghi tiep
    if _ban_left_sec() > 0:
        scan_status = _ban_status_text()
        print(f"[BAN] {scan_status} — bo qua vong quet nay, khong goi Binance.")
        return

    # [PATCH 2026-10-16d] Che do WS: chua nap du du lieu thi CHO, KHONG quet REST
    # (quay ve REST luc nay chinh la thu lam ban bi keo dai).
    if _ws_mode() and not _use_rest_scan():
        if not ws_feed.is_usable():
            scan_status = ws_feed.warmup_text()
            scan_debug  = {"stage": "ws_warmup", "ws": ws_feed.stats()}
            print(f"[WS] {scan_status} — bo qua vong quet nay (khong goi REST).")
            return

    print(f"\n🚀 QUÉT KÈO V74: {scan_time_str}")
    scan_status = f"Đang quét... ({scan_time_str})"

    found_signals = []
    dbg = {"start": scan_time_str, "pairs": 0, "scanned": 0, "errors": 0, "skipped": 0,
           "err_samples": [], "stage": "start", "outer_error": ""}
    scan_debug = dbg
    use_ws = _ws_mode() and not _use_rest_scan()

    try:
        if use_ws:
            # [PATCH 2026-10-16d] Danh sach cap lay tu feed WS — khong goi REST
            spot_pairs    = ws_feed.symbols()
            dbg["source"] = "ws"
            dbg["ws_ready"] = ws_feed.ready_count()
        else:
            markets    = exchange.load_markets()
            spot_pairs = [
                s for s, m in markets.items()
                if m['spot'] and m['quote'] == 'USDT' and m['active']
            ]
            dbg["source"] = "rest"

        # [PATCH 2026-10-17] Loai coin co thong bao delisting truoc khi quet
        delist_blocklist = get_delisting_blocklist()
        delist_removed = sorted({
            s.split('/')[0] for s in spot_pairs
            if s.split('/')[0] in delist_blocklist
        })
        if delist_removed:
            spot_pairs = [s for s in spot_pairs if s.split('/')[0] not in delist_blocklist]
            print(f"🚫 Bỏ qua {len(delist_removed)} coin có thông báo delisting: {delist_removed}")
        dbg["delist_blocked"] = delist_removed

        total = len(spot_pairs)
        dbg["pairs"] = total
        dbg["stage"] = "scanning"
        print(f"📋 Tổng số cặp USDT: {total} (nguồn: {dbg['source']})")

        for idx, symbol in enumerate(spot_pairs, 1):
            try:
                if use_ws:
                    # [PATCH 2026-10-16d] Lay nen tu bo dem WS, KHONG goi API
                    ohlcv = ws_feed.get_ohlcv(symbol)
                    if not ohlcv:
                        dbg["skipped"] += 1
                        continue
                else:
                    ohlcv = exchange.fetch_ohlcv(symbol, '4h', limit=150)

                # [PATCH 2026-10-18] Chi dung nen DA DONG - giong dung bot VPS
                # (projects/coin-spot-v74-2slot/bot.py::_final_candles). Nen cuoi
                # cung tu WS/REST co the con dang chay (chua dong), neu dung se
                # ra tin hieu/gia khac voi bot live va "nhay" moi lan quet.
                now_ms = time.time() * 1000
                ohlcv = [c for c in ohlcv if float(c[0]) + H4_MS <= now_ms]
                if len(ohlcv) < 96:
                    dbg["skipped"] += 1
                    continue
                dbg["scanned"] += 1
                df      = pd.DataFrame(ohlcv)
                close_p = df[4].apply(pd.to_numeric)
                high_p  = df[2].apply(pd.to_numeric)
                low_p   = df[3].apply(pd.to_numeric)
                vol_p   = df[5].apply(pd.to_numeric)

                recent_range = SCAN_DAYS * 6
                idx_peak     = high_p.tail(recent_range).idxmax()
                price_peak   = high_p.loc[idx_peak]

                peak_time        = datetime.fromtimestamp(df[0].loc[idx_peak] / 1000)
                days_since_peak  = (
                    (datetime.now() - peak_time).days
                    + (datetime.now() - peak_time).seconds / 86400
                )

                # [PATCH 2026-10-16c] Dinh phai nam trong khoang MIN..MAX ngay tro lai
                if days_since_peak > MAX_DAYS_FROM_PEAK or days_since_peak < MIN_DAYS_FROM_PEAK:
                    continue

                search_bottom_start = max(0, idx_peak - 60)
                price_bottom        = low_p.iloc[search_bottom_start:idx_peak].min()

                growth        = (price_peak / price_bottom) - 1
                current_price = close_p.iloc[-1]
                pullback      = (current_price / price_peak) - 1

                if growth * 100 < MIN_GROWTH_PCT:
                    continue
                if pullback * 100 > -MIN_PULLBACK_PCT:
                    continue

                ma20      = close_p.rolling(window=20).mean()
                std20     = close_p.rolling(window=20).std()
                bb_width  = (ma20 + std20 * 2) - (ma20 - std20 * 2)
                squeeze   = bb_width / ma20
                vol_ratio = vol_p.tail(5).mean() / vol_p.tail(20).mean()

                if squeeze.iloc[-1] < SQUEEZE_THRESHOLD and vol_ratio < VOL_EXHAUST_RATIO:
                    tp = ma20.iloc[-1]
                    sl = (ma20.iloc[-1] - (std20.iloc[-1] * 2.5)) * 0.98

                    table.add_row([
                        symbol.split('/')[0],
                        f"{growth * 100:.0f}",
                        f"{pullback * 100:.0f}",
                        f"{days_since_peak:.1f}d",
                        f"{squeeze.iloc[-1]:.2f}",
                        f"{vol_ratio:.2f}"
                    ])

                    found_signals.append({
                        'symbol':    symbol,
                        'df':        df,
                        'price':     round(float(current_price), 6),
                        'growth':    round(float(growth * 100), 1),
                        'pullback':  round(float(pullback * 100), 1),
                        'tp':        round(float(tp), 4),
                        'sl':        round(float(sl), 4),
                        'squeeze':   round(float(squeeze.iloc[-1]), 2),
                        'vol_ratio': round(float(vol_ratio), 2),
                        'days_peak': round(float(days_since_peak), 1),
                        'time':      datetime.now().strftime('%H:%M:%S'),
                    })

                time.sleep(0.01)

            except Exception as _e:
                # [PATCH 2026-10-16] Truoc day cho nay nuot loi im lang -> sua: dem + luu mau loi
                dbg["errors"] += 1
                _m = f"{type(_e).__name__}: {_e}"[:200]
                if _m not in dbg["err_samples"] and len(dbg["err_samples"]) < 5:
                    dbg["err_samples"].append(_m)
                # [PATCH 2026-10-16b] Bi ban IP -> dung quet ngay, khong goi tiep 495 cap
                if _note_ban(_m):
                    dbg["ban"] = True
                    dbg["ban_until"] = BAN_UNTIL_MS
                    dbg["stage"] = "banned"
                    print("[BAN] Binance chan IP — dung vong quet nay, nghi cho het ban.")
                    break
                continue

        # [PATCH 2026-10-16b] Dang bi ban -> KHONG ghi lich su "0 tin hieu" gay hieu nham
        if dbg.get("ban"):
            scan_debug  = dbg
            scan_status = _ban_status_text()
            print(f"[BAN] {scan_status}")
            return

        # ── Tạo danh sách signal JSON cho lần quét này ──────────
        signals_json = [
            {
                'symbol':    s['symbol'],
                'price':     s['price'],
                'growth':    s['growth'],
                'pullback':  s['pullback'],
                'tp':        s['tp'],
                'sl':        s['sl'],
                'squeeze':   s['squeeze'],
                'vol_ratio': s['vol_ratio'],
                'days_peak': s['days_peak'],
                'time':      s['time'],
            }
            for s in found_signals
        ]

        # ── Cập nhật biến toàn cục ───────────────────────────────
        latest_signals = signals_json
        last_scan_time = scan_time_str
        scan_status    = (f"Hoàn tất lúc {scan_time_str} — {len(found_signals)} tín hiệu"
                          f" ({dbg['source'].upper()}, {dbg['scanned']} cặp)")
        BAN_UNTIL_MS   = 0   # [PATCH 2026-10-16b] quet duoc tuc la da het ban
        if _ws_mode():
            dbg["ws"] = ws_feed.stats()

        # ── LƯU VÀO LỊCH SỬ (MỚI) ───────────────────────────────
        scan_history.insert(0, {
            "scan_time": scan_time_str,   # "HH:mm:ss dd/MM/YYYY"
            "signals":   signals_json
        })
        # Giữ tối đa MAX_HISTORY lần quét
        if len(scan_history) > MAX_HISTORY:
            scan_history.pop()

        print(f"📚 Lịch sử đang lưu: {len(scan_history)} lần quét")

        # ── GỬI TELEGRAM ────────────────────────────────────────
        # [PATCH 2026-10-16d] Che do WS quet rat thuong xuyen (mac dinh 15 phut)
        # nen phai chong spam: khong gui lai tin hieu cu, khong gui "0 tin hieu" lien tuc.
        if found_signals:
            print(table)
            send_list = [s for s in found_signals if _alert_allowed(s['symbol'])]
            if send_list:
                send_telegram_msg(
                    f"📊 *V74 REPORT — {scan_time_str}*\n"
                    f"Tìm thấy *{len(found_signals)}* tín hiệu "
                    f"({len(send_list)} mới)\n"
                    f"```\n{table.get_string()}\n```"
                )
            else:
                print("[TELEGRAM] Tin hieu da gui truoc do (trong ALERT_DEDUP_SEC) — khong gui lai.")
            for s in send_list:
                chart_path = save_v74_chart(s['symbol'], s['df'])
                caption = (
                    f"🎯 *SIGNAL: {s['symbol']}*\n"
                    f"🔥 Sóng Pump: `+{s['growth']:.1f}%` | Pullback: `{s['pullback']:.1f}%`\n"
                    f"💰 Giá hiện tại: `{s['price']}`\n"
                    f"📦 Squeeze: `{s['squeeze']:.2f}` | Vol: `{s['vol_ratio']:.2f}`\n"
                    f"⏱ Đỉnh cách đây: `{s['days_peak']:.1f} ngày`\n"
                    f"✅ TP: `{s['tp']:.4f}` | 🛑 SL: `{s['sl']:.4f}`"
                )
                send_telegram_photo(caption, chart_path)
                if os.path.exists(chart_path):
                    os.remove(chart_path)
        else:
            print("🔭 Không có mã nào thỏa mãn sóng Pump và độ nén chuẩn.")
            # [PATCH 2026-10-16d] Chi bao "0 tin hieu" moi NO_SIGNAL_NOTIFY_SEC
            if time.time() - last_no_signal_sent >= NO_SIGNAL_NOTIFY_SEC:
                last_no_signal_sent = time.time()
                send_telegram_msg(
                    f"🔭 *V74 — {scan_time_str}*\n"
                    f"Không có mã nào thỏa mãn điều kiện Pump + Squeeze."
                )

    except Exception as e:
        try:
            dbg["stage"] = "outer_error"
            dbg["outer_error"] = f"{type(e).__name__}: {e}"[:300]
            scan_debug = dbg
        except Exception:
            pass
        # [PATCH 2026-10-16b] Neu loi la ban IP -> ghi moc va nghi cho het ban
        if _note_ban(str(e)):
            scan_status = _ban_status_text()
            print(f"[BAN] {scan_status}")
        else:
            scan_status = f"Lỗi: {e}"
        print(f"[Lỗi quét] {e}")

# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    RUN_ONCE   = _os.environ.get("RUN_ONCE", "0") == "1"
    ENABLE_API = _os.environ.get("ENABLE_API", "1") == "1"

    print("=" * 50)
    print("  V74 BOT KHOI DONG")
    print(f"  Che do: {'QUET 1 LAN (CI)' if RUN_ONCE else 'CHAY LIEN TUC'}")
    print(f"  Flask API: {'BAT - port ' + str(FLASK_PORT) if ENABLE_API else 'TAT'}")
    print(f"  Luu lich su: {MAX_HISTORY} lan quet gan nhat")
    print("=" * 50)

    # Preflight: neu san chan IP (loi 451) thi FAIL RO, khong "xanh gia"
    if RUN_ONCE:
        try:
            _n = len(exchange.load_markets())
            print(f"  Ket noi san OK - {_n} markets")
        except Exception as _e:
            print(f"[FATAL] Khong ket noi duoc san: {_e}")
            print("[FATAL] Neu la loi 451 -> doi Variable EXCHANGE_ID sang kucoin/okx/bybit/gateio")
            _sys.exit(1)

    if ENABLE_API and not RUN_ONCE:
        flask_thread = threading.Thread(target=run_flask, daemon=True)
        flask_thread.start()
        print(f"Flask API dang chay tai port {FLASK_PORT}")

    if RUN_ONCE:
        run_v74_standard()
        print("Xong 1 lan quet, thoat.")
    else:
        while True:
            run_v74_standard()
            print(f"\nNghi {SCAN_INTERVAL // 60} phut, quet tiep sau "
                  f"{datetime.now().strftime('%H:%M:%S')}...")
            time.sleep(SCAN_INTERVAL)
