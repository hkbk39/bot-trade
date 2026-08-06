# -*- coding: utf-8 -*-
"""
CHAY 1 LAN DUY NHAT:  python patch_bots.py

Doc 2 file goc o thu muc cha, sinh ra bot_v74.py + bot_sentinel.py trong
thu muc nay, voi cac thay doi:
  1. TOKEN / CHAT_ID  ->  doc tu bien moi truong (khong con hardcode)
  2. Them RUN_ONCE    ->  quet 1 lan roi thoat (cho GitHub Actions)
  3. Them EXCHANGE_ID ->  doi san khong can sua code (tranh loi 451)
  4. matplotlib backend Agg -> ve chart duoc tren server khong man hinh
  5. Sentinel luu prev_btc_level ra file -> tin "BTC HOI PHUC" van bat duoc
  6. Vong lap Sentinel boc try/except -> loi mang khong lam chet bot
"""

import io
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)

SRC_V74 = os.path.join(PARENT, "Quecoinnendkmoi--run.py")
SRC_SEN = os.path.join(PARENT, "Quetbankhongclaude--run.py")

COMMON = '''
# ============================================================
# [DEPLOY PATCH] CAU HINH TU BIEN MOI TRUONG - KHONG HARDCODE TOKEN
# ============================================================
import os as _os
import sys as _sys

# Doc file .env neu co (chi chay o may local; tren GitHub/Render thi bo qua)
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv()
except ImportError:
    pass


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
'''

# Bat moi token Telegram dang <so>:<chuoi> - khong can viet token that vao day
TOKEN_RE = re.compile(r'\b\d{8,12}:[A-Za-z0-9_-]{30,}\b')


def read(path):
    if not os.path.exists(path):
        sys.exit(f"[LOI] Khong tim thay file goc:\n  {path}\n"
                 f"Sua bien SRC_V74 / SRC_SEN o dau file nay cho dung duong dan.")
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return io.open(path, encoding=enc).read(), enc
        except UnicodeDecodeError:
            continue
    sys.exit(f"[LOI] Khong doc duoc {path}")


def must(cond, msg):
    if not cond:
        sys.exit(f"[LOI] {msg}")


# ─────────────────────────── BOT V74 ───────────────────────────
s, enc = read(SRC_V74)
print(f"Doc {os.path.basename(SRC_V74)} (encoding {enc})")

s = re.sub(
    r'TOKEN\s*=\s*["\'][^"\']+["\']\s*\nCHAT_ID\s*=\s*["\'][^"\']+["\']',
    lambda m: COMMON + '\nTOKEN   = _env("TELEGRAM_TOKEN", required=True)\n'
                       'CHAT_ID = _env("TELEGRAM_CHAT_ID", required=True)',
    s, count=1
)
must('_env("TELEGRAM_TOKEN"' in s, "v74: khong tim thay khoi TOKEN/CHAT_ID")

s = s.replace(
    "FLASK_PORT   = 5000",
    'FLASK_PORT   = int(_os.environ.get("PORT", 5000))   # Render tu cap PORT'
)
s = s.replace("exchange = ccxt.binance()", "exchange = _build_exchange()")
must("_build_exchange()" in s, "v74: khong tim thay dong khoi tao exchange")

s = s.replace(
    "import mplfinance as mpf",
    'import matplotlib\nmatplotlib.use("Agg")   # chay khong can man hinh (CI/server)\n'
    "import mplfinance as mpf"
)

# Bao loi ro khi Telegram tu choi (token sai / chat_id sai)
s = re.sub(
    r'( +)requests\.post\(url, json=payload, timeout=10\)',
    lambda m: f"{m.group(1)}_r = requests.post(url, json=payload, timeout=10)\n"
              f"{m.group(1)}if _r.status_code != 200:\n"
              f"{m.group(1)}    print(f\"[TELEGRAM LOI HTTP {{_r.status_code}}] {{_r.text[:300]}}\")",
    s, count=1
)

must('if __name__ == "__main__":' in s, "v74: khong tim thay entry point")
s = s[:s.index('if __name__ == "__main__":')] + '''if __name__ == "__main__":
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
            print(f"\\nNghi 1 tieng, quet tiep sau "
                  f"{datetime.now().strftime('%H:%M:%S')}...")
            time.sleep(3600)
'''

must(not TOKEN_RE.search(s), "v74: VAN CON token hardcode!")
out = os.path.join(HERE, "bot_v74.py")
io.open(out, "w", encoding="utf-8").write(s)
print(f"  -> da tao {out}")


# ───────────────────────── BOT SENTINEL ─────────────────────────
s, enc = read(SRC_SEN)
print(f"Doc {os.path.basename(SRC_SEN)} (encoding {enc})")

s = re.sub(
    r'TELEGRAM_TOKEN\s*=\s*["\'][^"\']+["\']\s*\nCHAT_ID\s*=\s*["\'][^"\']+["\']',
    lambda m: COMMON + '\nTELEGRAM_TOKEN = _env("TELEGRAM_TOKEN", required=True)\n'
                       'CHAT_ID        = _env("TELEGRAM_CHAT_ID", required=True)',
    s, count=1
)
must('_env("TELEGRAM_TOKEN"' in s, "sentinel: khong tim thay khoi TOKEN/CHAT_ID")

s = s.replace(
    """exchange = ccxt.binance({
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'},
})""",
    "exchange = _build_exchange()"
)
must("_build_exchange()" in s, "sentinel: khong tim thay dong khoi tao exchange")

s = s.replace("_prev_btc_level = 0\n", '''_STATE_FILE = _os.environ.get("STATE_FILE", "sentinel_state.json")


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
''', 1)
must("_load_state()" in s, "sentinel: khong tim thay _prev_btc_level")

# Cho phep giam so coin quet qua bien moi truong (RAM tren Render free chi 512MB)
s = re.sub(r'LIMIT_SCAN(\s*)=(\s*)\d+',
           lambda m: f'LIMIT_SCAN{m.group(1)}={m.group(2)}int(_os.environ.get("LIMIT_SCAN", 300))',
           s, count=1)
must('_os.environ.get("LIMIT_SCAN"' in s, "sentinel: khong tim thay LIMIT_SCAN")

# Bao loi ro khi Telegram tu choi (token sai / chat_id sai)
s = s.replace("""        requests.post(url, json={
            'chat_id':    CHAT_ID,
            'text':       text,
            'parse_mode': 'Markdown',
        }, timeout=15)""", """        _r = requests.post(url, json={
            'chat_id':    CHAT_ID,
            'text':       text,
            'parse_mode': 'Markdown',
        }, timeout=15)
        if _r.status_code != 200:
            print(f"  [TELEGRAM LOI HTTP {_r.status_code}] {_r.text[:300]}")""", 1)

s = s.replace("        _prev_btc_level = btc_level\n",
              "        _prev_btc_level = btc_level\n        _save_state(btc_level)\n")
s = s.replace("        _prev_btc_level = 0\n\n    print(",
              "        _prev_btc_level = 0\n        _save_state(0)\n\n    print(")

must('if __name__ == "__main__":' in s, "sentinel: khong tim thay entry point")
s = s[:s.index('if __name__ == "__main__":')] + '''if __name__ == "__main__":
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
'''

must(not TOKEN_RE.search(s), "sentinel: VAN CON token hardcode!")
out = os.path.join(HERE, "bot_sentinel.py")
io.open(out, "w", encoding="utf-8").write(s)
print(f"  -> da tao {out}")

print("\nXONG. Kiem tra nhanh:")
print("  python -m py_compile bot_v74.py bot_sentinel.py")
print("  findstr /C:\":AAG\" bot_v74.py bot_sentinel.py   (phai KHONG ra ket qua)")
