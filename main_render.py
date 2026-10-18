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
from datetime import datetime, timedelta

# Hai module bot không tự khởi động gì khi được import
# (mọi thứ đều nằm trong khối `if __name__ == "__main__"`).
import bot_v74

# Sentinel mặc định TẮT ở đây vì nó đã chạy trên GitHub Actions (cron 1h).
# Bật lại bằng biến môi trường ENABLE_SENTINEL=1 nếu muốn gộp cả 2 vào Render.
ENABLE_SENTINEL = os.environ.get("ENABLE_SENTINEL", "0") == "1"
if ENABLE_SENTINEL:
    import bot_sentinel

# [PATCH 2026-10-18] Theo yeu cau user: quet/gui du lieu DUNG 4 tieng 1 lan, khop
# moc nen H4 dong (00/04/08/12/16/20h UTC = 07/11/15/19/23/03h VN +1 phut de chac
# chan nen moi da dong). Khong con dung V74_INTERVAL (nghi co dinh) cho vong V74
# nua - gia tri nay chi con y nghia lich su, khong anh huong lich quet.
V74_SCHEDULE_HOURS  = [int(x) for x in os.environ.get("V74_SCHEDULE_HOURS", "3,7,11,15,19,23").split(",")]
V74_SCHEDULE_MINUTE = int(os.environ.get("V74_SCHEDULE_MINUTE", "1"))
V74_INTERVAL      = int(os.environ.get("V74_INTERVAL", 7200))       # giây (khong con dung cho vong V74)
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


def _seconds_until_next_v74_mark(now=None):
    """[PATCH 2026-10-18] Giay con lai toi moc HH:MINUTE ke tiep trong
    V74_SCHEDULE_HOURS (gio dia phuong - Render dat TZ=Asia/Ho_Chi_Minh).
    """
    now = now or datetime.now()
    candidates = []
    for day_offset in (0, 1):
        day = now + timedelta(days=day_offset)
        for h in V74_SCHEDULE_HOURS:
            cand = day.replace(hour=h, minute=V74_SCHEDULE_MINUTE, second=0, microsecond=0)
            if cand > now:
                candidates.append(cand)
    nxt = min(candidates)
    return max(1.0, (nxt - now).total_seconds()), nxt


def _loop_v74(fns):
    """[PATCH 2026-10-18] Vong quet V74 chay DUNG cac moc gio co dinh (mac dinh
    7h01/11h01/15h01/19h01/23h01/3h01) thay vi nghi 1 khoang thoi gian co dinh
    tu luc service khoi dong - khop voi nhip nen H4 dong va voi lich gui tin
    Telegram cua scan_v74_live.py (cron agent coin).

    [PATCH 2026-10-18 them N7] fns la 1 list ham quet, chay LAN LUOT tai CUNG
    1 moc gio (V74 roi N7) de 2 keo luon dong bo lich, khong tao vong lap/lich
    rieng cho N7."""
    while True:
        wait, nxt = _seconds_until_next_v74_mark()
        print(f"[V74] ⏸ chờ tới mốc {nxt:%H:%M:%S %d/%m} ({wait / 60:.1f} phút)")
        time.sleep(wait)
        started = datetime.now().strftime("%H:%M:%S %d/%m/%Y")
        print(f"\n[V74] ▶ bắt đầu quét lúc {started}")
        for fn in fns:
            try:
                fn()
            except Exception as e:
                print(f"[V74] ✖ LỖI ({fn.__name__}): {e}")
                traceback.print_exc()


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

        # [PATCH 2026-10-16b] Dang bi Binance ban: chi cho toi luc het ban (+60s)
        # thay vi ngu du 2 tieng -> bot tu quet lai ngay khi duoc mo ban.
        wait = interval
        try:
            if hasattr(bot_v74, "_ban_left_sec"):
                left = bot_v74._ban_left_sec()
                if left > 0:
                    wait = min(interval, left + 60)
        except Exception:
            wait = interval
        print(f"[{name}] ⏸ nghỉ {wait // 60} phút")
        time.sleep(wait)


def _start_ws_feed():
    """[PATCH 2026-10-16d] Khoi dong nguon du lieu WebSocket ngay khi service lên.

    Backfill chạy nền (~60 request/phút) nên phải bắt đầu TRƯỚC vòng quét đầu
    tiên; nếu chưa xong, bot_v74 sẽ bỏ qua vòng đó chứ KHÔNG gọi REST.
    """
    feed = getattr(bot_v74, "ws_feed", None)
    if feed is None:
        print("[WS] Khong bat WebSocket (dang dung REST)")
        return
    try:
        feed.start()
        print(f"[WS] Da bat WebSocket — nguồn dữ liệu: {bot_v74.V74_DATA_SOURCE}")
    except Exception as e:
        print(f"[WS] Khong khoi dong duoc WS: {e}")


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
    print(f"  Chu kỳ V74: đúng mốc {V74_SCHEDULE_HOURS}h:{V74_SCHEDULE_MINUTE:02d} (giờ VN)")
    print(f"  Sentinel:   {'BAT (gop chung)' if ENABLE_SENTINEL else 'TAT (chay tren GitHub Actions)'}")
    print(f"  Du lieu:    {getattr(bot_v74, 'V74_DATA_SOURCE', 'rest').upper()} "
          f"(WS = khong ton request weight)")
    print("=" * 60)

    threading.Thread(target=_self_ping, daemon=True, name="selfping").start()

    # [PATCH 2026-10-16d] Bat WebSocket TRUOC vong quet dau tien
    threading.Thread(target=_start_ws_feed, daemon=True, name="wsfeed").start()

    threading.Thread(
        target=_loop_v74, args=([bot_v74.run_v74_standard, bot_v74.run_n7_standard],),
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
