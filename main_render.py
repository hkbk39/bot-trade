# ============================================================
#  RUNNER GỘP — chạy CẢ 2 BOT + Flask API trong 1 tiến trình
#
#  Dùng cho Render / Fly.io / Koyeb / VPS — nơi cần 1 web service
#  chạy 24/7 và có endpoint /signals cho app Android.
#
#  Chạy:  python main_render.py
# ============================================================

import os
import time
import threading
import traceback
from datetime import datetime

# Hai module bot không tự khởi động gì khi được import
# (mọi thứ đều nằm trong khối `if __name__ == "__main__"`).
import bot_v74

# Sentinel mặc định TẮT ở đây vì nó đã chạy trên GitHub Actions (cron 1h).
# Bật lại bằng biến môi trường ENABLE_SENTINEL=1 nếu muốn gộp cả 2 vào Render.
ENABLE_SENTINEL = os.environ.get("ENABLE_SENTINEL", "0") == "1"
if ENABLE_SENTINEL:
    import bot_sentinel

V74_INTERVAL      = int(os.environ.get("V74_INTERVAL", 3600))       # giây
SENTINEL_INTERVAL = int(os.environ.get("SENTINEL_INTERVAL", 3600))  # giây
SENTINEL_DELAY    = int(os.environ.get("SENTINEL_DELAY", 300))      # lệch giờ so với V74
PORT              = int(os.environ.get("PORT", 5000))


def _self_ping():
    """Tự gọi vào URL công khai của chính mình để Render không cho service ngủ.

    Render cấp sẵn biến RENDER_EXTERNAL_URL. Request đi ra internet rồi vòng
    về qua load balancer nên được tính là traffic thật — khác với health check
    nội bộ (10.x.x.x) vốn KHÔNG ngăn được spin-down.
    """
    import urllib.request

    url = os.environ.get("SELF_URL") or os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        print("[PING] Khong co RENDER_EXTERNAL_URL — bo qua tu ping")
        return

    url = url.rstrip("/") + "/health"
    interval = int(os.environ.get("PING_INTERVAL", 600))   # 10 phút < 15 phút
    print(f"[PING] Tu ping {url} moi {interval}s")

    time.sleep(60)   # chờ Flask sẵn sàng
    while True:
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                print(f"[PING] {r.status} lúc {datetime.now():%H:%M:%S}")
        except Exception as e:
            print(f"[PING] lỗi: {e}")
        time.sleep(interval)


def _loop(name, fn, interval, first_delay=0):
    if first_delay:
        print(f"[{name}] chờ {first_delay}s rồi bắt đầu...")
        time.sleep(first_delay)
    while True:
        started = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
        print(f"\n[{name}] ▶ bắt đầu quét lúc {started}")
        try:
            fn()
        except Exception as e:
            print(f"[{name}] ✖ LỖI: {e}")
            traceback.print_exc()
        print(f"[{name}] ⏸ nghỉ {interval // 60} phút")
        time.sleep(interval)


# Endpoint để dịch vụ ping giữ service không ngủ (Render free ngủ sau 15' không có request)
@bot_v74.flask_app.route("/")
def _root():
    return {
        "ok": True,
        "service": "bot-trade",
        "bots": ["v74", "sentinel"],
        "time": datetime.now().strftime("%H:%M:%S %d/%m/%Y"),
    }


if __name__ == "__main__":
    print("=" * 60)
    print("  RUNNER — V74 + Flask API")
    print(f"  API:       http://0.0.0.0:{PORT}/signals")
    print(f"  Keepalive: http://0.0.0.0:{PORT}/health")
    print(f"  Chu kỳ V74: {V74_INTERVAL // 60} phút")
    print(f"  Sentinel:   {'BAT (gop chung)' if ENABLE_SENTINEL else 'TAT (chay tren GitHub Actions)'}")
    print("=" * 60)

    threading.Thread(target=_self_ping, daemon=True, name="selfping").start()

    threading.Thread(
        target=_loop, args=("V74", bot_v74.run_v74_standard, V74_INTERVAL, 0),
        daemon=True, name="v74",
    ).start()

    if ENABLE_SENTINEL:
        threading.Thread(
            target=_loop,
            args=("SENTINEL", bot_sentinel.run_sentinel, SENTINEL_INTERVAL, SENTINEL_DELAY),
            daemon=True, name="sentinel",
        ).start()

    # Flask giữ tiến trình chính sống
    bot_v74.flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
