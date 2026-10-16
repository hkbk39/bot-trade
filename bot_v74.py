import ccxt
import pandas as pd
import requests
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
MIN_GROWTH_PCT   = 40
MAX_DAYS_FROM_PEAK = 9
MIN_PULLBACK_PCT = 30
SQUEEZE_THRESHOLD = 0.3
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

    table = PrettyTable()
    table.field_names = ["Coin", "Up%", "Down%", "Peak", "Sqze", "Vol"]
    table.align = "l"

    scan_time_str = datetime.now().strftime('%H:%M:%S %d/%m/%Y')

    # [PATCH 2026-10-16b] Dang trong thoi gian bi ban -> KHONG goi API, nghi tiep
    if _ban_left_sec() > 0:
        scan_status = _ban_status_text()
        print(f"[BAN] {scan_status} — bo qua vong quet nay, khong goi Binance.")
        return

    print(f"\n🚀 QUÉT KÈO V74: {scan_time_str}")
    scan_status = f"Đang quét... ({scan_time_str})"

    found_signals = []
    dbg = {"start": scan_time_str, "pairs": 0, "scanned": 0, "errors": 0,
           "err_samples": [], "stage": "start", "outer_error": ""}
    scan_debug = dbg

    try:
        markets    = exchange.load_markets()
        spot_pairs = [
            s for s, m in markets.items()
            if m['spot'] and m['quote'] == 'USDT' and m['active']
        ]

        total = len(spot_pairs)
        dbg["pairs"] = total
        dbg["stage"] = "scanning"
        print(f"📋 Tổng số cặp USDT: {total}")

        for idx, symbol in enumerate(spot_pairs, 1):
            try:
                ohlcv   = exchange.fetch_ohlcv(symbol, '4h', limit=150)
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

                if days_since_peak > MAX_DAYS_FROM_PEAK or days_since_peak < 1.5:
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
        scan_status    = f"Hoàn tất lúc {scan_time_str} — {len(found_signals)} tín hiệu"
        BAN_UNTIL_MS   = 0   # [PATCH 2026-10-16b] quet duoc tuc la da het ban

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
        if found_signals:
            print(table)
            send_telegram_msg(
                f"📊 *V74 REPORT — {scan_time_str}*\n"
                f"Tìm thấy *{len(found_signals)}* tín hiệu\n"
                f"```\n{table.get_string()}\n```"
            )
            for s in found_signals:
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
            msg = (
                f"🔭 *V74 — {scan_time_str}*\n"
                f"Không có mã nào thỏa mãn điều kiện Pump + Squeeze."
            )
            print("🔭 Không có mã nào thỏa mãn sóng Pump và độ nén chuẩn.")
            send_telegram_msg(msg)

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
