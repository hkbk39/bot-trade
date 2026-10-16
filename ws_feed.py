# ============================================================
#  ws_feed.py — NGUỒN DỮ LIỆU BINANCE QUA WEBSOCKET
#
#  Vì sao có file này:
#   Bot V74 cần nến 4H của ~500 cặp USDT. Lấy bằng REST (fetch_ohlcv) tốn
#   "request weight" -> IP bị Binance trả 418 "IP banned", và ban bị KÉO DÀI
#   nếu bot vẫn tiếp tục gọi. WebSocket thì Binance ĐẨY dữ liệu về, KHÔNG tốn
#   weight -> không thể bị ban vì quét.
#
#  Cách hoạt động:
#   1) Khám phá symbol: mở wss !miniTicker@arr, đọc 1 bản tin -> danh sách
#      các cặp USDT (nếu stream này lỗi thì fallback REST load_markets 1 lần).
#   2) Backfill: nạp 150 nến 4H cho từng cặp bằng REST nhưng RẤT CHẬM
#      (mặc định 60 request/phút => ~8-10 phút cho 500 cặp), và tự dừng nếu
#      gặp 418 rồi chờ hết ban.
#   3) Duy trì 1 kết nối WS, subscribe <symbol>@kline_4h (chia lô 100 stream),
#      cập nhật bộ đệm nến trong RAM.
#   4) Watchdog: im lặng quá lâu -> đóng kết nối, nối lại, subscribe lại.
#
#  Lưu ý: KHÔNG import bot_v74 (tránh import vòng). Exchange (ccxt) được
#  truyền vào từ bên ngoài, chỉ dùng cho bước backfill.
# ============================================================

import json
import os
import threading
import time
from datetime import datetime

try:
    import websocket   # package: websocket-client
    WS_AVAILABLE = True
except Exception:       # pragma: no cover
    websocket = None
    WS_AVAILABLE = False


# Các loại token đòn bẩy / không phải spot thường -> bỏ khỏi danh sách quét
_BAD_SUFFIX = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")

_INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
}


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class BinanceKlineFeed:
    """Bộ đệm nến Binance cập nhật liên tục qua WebSocket."""

    def __init__(self, interval="4h", limit=150, rest=None, symbols=None,
                 base_url=None, backfill_rpm=60.0, min_candles=60,
                 stale_sec=180, recv_timeout=30, max_streams=1024, min_ready=10):
        self.interval  = interval
        self.limit     = int(limit)
        self.min_candles = int(min_candles)
        self.min_ready = int(min_ready)
        self.rest      = rest
        self.base      = (base_url
                          or os.environ.get("WS_BASE_URL", "wss://stream.binance.com:9443")).rstrip("/")
        self.backfill_rpm = max(1.0, float(backfill_rpm))
        self.stale_sec = float(stale_sec)
        self.recv_timeout = float(recv_timeout)
        self.max_streams = int(max_streams)

        self._lock      = threading.RLock()
        self._buf       = {}            # symbol -> [[t,o,h,l,c,v], ...]
        self._symbols   = list(symbols or [])
        self._backfilled = set()        # symbol da nap du lich su
        self._ws        = None
        self._connected = False
        self._connected_once = False
        self._last_msg  = 0.0
        self._started   = 0.0
        self._state     = "chưa khởi động"
        self._errors    = []
        self._err_count = 0
        self._msg_count = 0
        self._reconnects = 0
        self._banned_until = 0.0        # epoch ms
        self._stop      = False
        self._threads   = []

    # ------------------------------------------------------------
    #  VÒNG ĐỜI
    # ------------------------------------------------------------
    def start(self):
        if self._started:
            return self
        self._started = time.time()
        if not WS_AVAILABLE:
            self._state = "thiếu thư viện websocket-client"
            self._record_error(self._state)
            return self
        self._state = "đang khởi động"
        for fn, name in ((self._discover_loop, "ws-discover"),
                         (self._ws_loop, "ws-kline"),
                         (self._backfill_loop, "ws-backfill"),
                         (self._watchdog_loop, "ws-watchdog")):
            t = threading.Thread(target=fn, name=name, daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def stop(self):
        self._stop = True
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    # ------------------------------------------------------------
    #  TIỆN ÍCH NỘI BỘ
    # ------------------------------------------------------------
    def _record_error(self, msg):
        self._err_count += 1
        m = str(msg)[:200]
        if m not in self._errors:
            self._errors.append(m)
            if len(self._errors) > 5:
                self._errors.pop(0)
        print(f"[WS] {m}")

    @staticmethod
    def _extract_ban_until(text):
        """Tìm 'banned until <ms>' (chấp nhận cả giây) -> epoch ms hoặc None."""
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
        if v < 10 ** 11:
            v *= 1000
        return v

    def _note_ban(self, text):
        ms = self._extract_ban_until(text)
        if not ms:
            return False
        if ms > self._banned_until:
            self._banned_until = float(ms)
        return True

    def _ban_left_sec(self):
        return max(0, int((self._banned_until - time.time() * 1000) / 1000))

    # ------------------------------------------------------------
    #  1) KHÁM PHÁ SYMBOL (không tốn weight)
    # ------------------------------------------------------------
    def _discover_loop(self):
        if self._symbols:
            return
        for attempt in range(1, 6):
            if self._stop:
                return
            try:
                syms = self._discover_via_ws()
                if syms:
                    with self._lock:
                        self._symbols = syms[: self.max_streams]
                    print(f"[WS] Khám phá được {len(self._symbols)} cặp USDT qua !miniTicker@arr")
                    return
            except Exception as e:
                self._record_error(f"khám phá symbol lỗi: {e}")
            time.sleep(min(30, 5 * attempt))

        # Fallback: 1 request REST load_markets (weight nhỏ, chấp nhận được)
        try:
            if self.rest is not None:
                mk = self.rest.load_markets()
                syms = sorted(
                    s for s, m in mk.items()
                    if m.get("spot") and m.get("quote") == "USDT" and m.get("active")
                    and not s.replace("/", "").endswith(_BAD_SUFFIX)
                )
                with self._lock:
                    self._symbols = syms[: self.max_streams]
                print(f"[WS] Khám phá qua REST: {len(self._symbols)} cặp USDT")
        except Exception as e:
            self._record_error(f"khám phá symbol qua REST lỗi: {e}")

    def _discover_via_ws(self):
        url = f"{self.base}/ws/!miniTicker@arr"
        ws = websocket.create_connection(url, timeout=20)
        try:
            raw = ws.recv()
        finally:
            try:
                ws.close()
            except Exception:
                pass
        data = json.loads(raw)
        items = data.get("data", data) if isinstance(data, dict) else data
        syms = []
        for it in items or []:
            s = (it or {}).get("s") or ""
            if s.endswith("USDT") and not s.endswith(_BAD_SUFFIX) and s not in syms:
                syms.append(s)
        return syms

    # ------------------------------------------------------------
    #  2) BACKFILL LỊCH SỬ — chậm, có kiểm soát
    # ------------------------------------------------------------
    def _backfill_loop(self):
        # Chờ có danh sách symbol
        for _ in range(600):
            if self._stop:
                return
            with self._lock:
                have = list(self._symbols)
            if have:
                break
            time.sleep(1)

        min_gap = 60.0 / self.backfill_rpm
        print(f"[WS] Bắt đầu nạp lịch sử {self.limit} nến {self.interval} "
              f"cho {len(have)} cặp — tối đa {self.backfill_rpm:.0f} request/phút")

        todo = [s for s in have if s not in self._backfilled]
        i = 0
        while i < len(todo) and not self._stop:
            sym = todo[i]
            if self._ban_left_sec() > 0:
                wait = self._ban_left_sec() + 60
                print(f"[WS] Đang bị Binance ban — tạm dừng nạp lịch sử {wait // 60} phút")
                time.sleep(min(wait, 3600))
                continue
            t0 = time.time()
            try:
                ohlcv = self.rest.fetch_ohlcv(sym, self.interval, limit=self.limit)
                self._merge_candles(sym, [[float(x) if j else int(x) for j, x in enumerate(c)]
                                          for c in ohlcv], from_ws=False)
                with self._lock:
                    self._backfilled.add(sym)
                i += 1
            except Exception as e:
                m = f"{type(e).__name__}: {e}"
                if self._note_ban(m):
                    print(f"[WS] Gặp 418 khi nạp lịch sử {sym} — nghỉ tới hết ban")
                    continue
                self._record_error(f"backfill {sym}: {m}")
                time.sleep(2)
                i += 1     # bỏ qua cặp lỗi, lần khởi động sau thử lại
            gap = min_gap - (time.time() - t0)
            if gap > 0:
                time.sleep(gap)

        with self._lock:
            left = len([s for s in self._symbols if s not in self._backfilled])
        print(f"[WS] Nạp lịch sử xong ({len(self._backfilled)} cặp, còn {left})")

    # ------------------------------------------------------------
    #  3) KẾT NỐI WS + SUBSCRIBE KLINE
    # ------------------------------------------------------------
    def _stream_symbols(self):
        with self._lock:
            return list(self._symbols)[: self.max_streams]

    def _ws_loop(self):
        backoff = 2
        while not self._stop:
            started = time.time()
            try:
                self._run_connection()
                backoff = 2
            except Exception as e:
                self._connected = False
                self._record_error(f"kết nối: {e}")
                self._state = f"mất kết nối: {str(e)[:150]}"
                if time.time() - started > 60:
                    backoff = 2
                time.sleep(backoff)
                backoff = min(60, backoff * 2)

    def _run_connection(self):
        syms = self._stream_symbols()
        if not syms:
            raise RuntimeError("chưa có danh sách symbol")
        url = f"{self.base}/stream"
        ws = websocket.create_connection(url, timeout=self.recv_timeout,
                                         enable_multithread=True)
        self._ws = ws
        sent = 0
        for batch in _chunks(syms, 100):
            params = [f"{s.replace('/', '').lower()}@kline_{self.interval}" for s in batch]
            ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
            sent += len(params)
            time.sleep(0.05)
        self._connected = True
        self._connected_once = True
        self._last_msg = time.time()
        self._state = f"đã kết nối — {sent} stream kline {self.interval}"
        print(f"[WS] {self._state}")

        while not self._stop:
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                try:
                    ws.ping()
                except Exception as e:
                    raise RuntimeError(f"ping thất bại: {e}")
                idle = time.time() - self._last_msg
                if idle > self.stale_sec:
                    raise RuntimeError(f"im lặng {int(idle)}s — nối lại")
                continue
            if raw is None or raw == "":
                raise RuntimeError("server đóng kết nối")
            self._last_msg = time.time()
            self._msg_count += 1
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            data = msg.get("data", msg) if isinstance(msg, dict) else {}
            k = data.get("k") if isinstance(data, dict) else None
            if not k:
                continue
            sym = data.get("s") or k.get("s")
            if not sym:
                continue
            try:
                candle = [int(k["t"]), float(k["o"]), float(k["h"]),
                          float(k["l"]), float(k["c"]), float(k["v"])]
            except Exception:
                continue
            self._apply_kline(sym, candle)

    # ------------------------------------------------------------
    #  4) WATCHDOG
    # ------------------------------------------------------------
    def _watchdog_loop(self):
        while not self._stop:
            time.sleep(15)
            try:
                if self._connected and self._last_msg:
                    idle = time.time() - self._last_msg
                    if idle > self.stale_sec:
                        print(f"[WS] Watchdog: im lặng {int(idle)}s — ngắt để nối lại")
                        self._reconnects += 1
                        ws = self._ws
                        if ws is not None:
                            try:
                                ws.close()
                            except Exception:
                                pass
            except Exception as e:
                self._record_error(f"watchdog: {e}")

    # ------------------------------------------------------------
    #  BỘ ĐỆM NẾN
    # ------------------------------------------------------------
    def _apply_kline(self, sym, candle):
        """Cập nhật nến đang chạy / nến mới (đường nóng, gọi mỗi bản tin WS)."""
        with self._lock:
            cur = self._buf.get(sym)
            if not cur:
                self._buf[sym] = [candle]
                return
            last = cur[-1]
            if candle[0] == last[0]:
                cur[-1] = candle
            elif candle[0] > last[0]:
                cur.append(candle)
                if len(cur) > self.limit:
                    del cur[0:len(cur) - self.limit]
            else:
                i = len(cur) - 1
                while i >= 0 and cur[i][0] > candle[0]:
                    i -= 1
                if i >= 0 and cur[i][0] == candle[0]:
                    cur[i] = candle
                else:
                    cur.insert(i + 1, candle)

    def _merge_candles(self, sym, candles, from_ws=True):
        """Gộp lịch sử (backfill) vào bộ đệm. Nến trùng mốc thời gian do WS giữ
        vì dữ liệu WS luôn mới hơn bản snapshot REST."""
        if not candles:
            return
        with self._lock:
            cur = self._buf.get(sym)
            if not cur:
                self._buf[sym] = list(candles)[-self.limit:]
                return
            by_ts = {c[0]: c for c in candles}
            for c in cur:
                by_ts[c[0]] = c
            merged = [by_ts[t] for t in sorted(by_ts)]
            self._buf[sym] = merged[-self.limit:]

    # ------------------------------------------------------------
    #  ĐỌC DỮ LIỆU
    # ------------------------------------------------------------
    def get_ohlcv(self, symbol, require_fresh=True):
        """Trả về danh sách nến [ts,o,h,l,c,v] giống ccxt, hoặc None nếu chưa đủ."""
        with self._lock:
            buf = self._buf.get(symbol)
            if not buf or len(buf) < self.min_candles:
                return None
            data = list(buf)
        if require_fresh:
            span = _INTERVAL_MS.get(self.interval, 14_400_000)
            now_ms = time.time() * 1000
            if now_ms - data[-1][0] > 2 * span + 120_000:
                return None      # dữ liệu cũ quá 2 nến -> không dùng để ra tín hiệu
        return [list(c) for c in data]

    def symbols(self):
        with self._lock:
            return list(self._symbols)

    def ready_count(self):
        with self._lock:
            return sum(1 for s, b in self._buf.items() if len(b) >= self.min_candles)

    def is_fresh(self):
        return bool(self._connected) and (time.time() - self._last_msg) < self.stale_sec

    def is_usable(self):
        """Đủ điều kiện để bot quét bằng WS (không cần REST)."""
        if not self.is_fresh():
            return False
        with self._lock:
            total = len(self._symbols)
        if total == 0:
            return False
        need = max(1, min(self.min_ready, total))
        return self.ready_count() >= need

    def failed_hard(self):
        """WS không thể dùng được nữa -> bot nên quay về REST.

        Lưu ý quan trọng: nếu đang bị Binance ban IP thì "WS chưa kết nối" là do
        ban, KHÔNG phải WS hỏng — tuyệt đối không được quay về REST lúc này.
        """
        if not WS_AVAILABLE:
            return True
        if self._connected_once:
            return False
        if self._ban_left_sec() > 0:
            return False
        return (time.time() - self._started) > 300 and self._err_count > 5

    def warmup_text(self):
        with self._lock:
            total = len(self._symbols)
        return (f"Đang nạp dữ liệu qua WebSocket: {self.ready_count()}/{total} cặp "
                f"({self._state})")

    def stats(self):
        with self._lock:
            total = len(self._symbols)
            backfilled = len(self._backfilled)
            nbuf = len(self._buf)
        age = int(time.time() - self._last_msg) if self._last_msg else None
        return {
            "library": WS_AVAILABLE,
            "state": self._state,
            "connected": self._connected,
            "interval": self.interval,
            "symbols": total,
            "buffers": nbuf,
            "ready": self.ready_count(),
            "backfilled": backfilled,
            "msg_count": self._msg_count,
            "last_msg_age_sec": age,
            "reconnects": self._reconnects,
            "errors": self._err_count,
            "err_samples": self._errors,
            "ban_left_sec": self._ban_left_sec(),
            "uptime_sec": int(time.time() - self._started) if self._started else 0,
        }
