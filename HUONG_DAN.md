# Hướng dẫn chạy 2 bot miễn phí + ẩn token Telegram

---

## ⚠️ BƯỚC 0 — LÀM NGAY TRƯỚC MỌI THỨ KHÁC

Token Telegram cũ nằm hardcode trong 2 file gốc (`Quecoinnendkmoi--run.py`, `Quetbankhongclaude--run.py`) và **đã bị lộ**. Bất kỳ ai có token đó đều chiếm được quyền bot của bạn.

**Thu hồi và lấy token mới:**

1. Mở Telegram → chat với **@BotFather**
2. Gõ `/mybots` → chọn bot của bạn → **API Token** → **Revoke current token**
3. BotFather trả về token mới → **chỉ dán vào Secrets, không bao giờ dán vào file .py**

Nếu bạn bỏ qua bước này thì mọi việc ẩn token phía dưới đều vô nghĩa.

---

---

## BƯỚC 1 — Sinh 2 file bot đã vá

Mở PowerShell:

```powershell
cd "D:\Code python\Bot trade\deploy-github"
python patch_bots.py
```

Script đọc `Quecoinnendkmoi--run.py` và `Quetbankhongclaude--run.py` ở thư mục cha, rồi sinh ra `bot_v74.py` + `bot_sentinel.py` ngay trong thư mục này. File gốc **không bị đụng vào**.

Kiểm tra:

```powershell
python -m py_compile bot_v74.py bot_sentinel.py
findstr /C:":AAG" bot_v74.py bot_sentinel.py
```

Lệnh `findstr` phải **không ra kết quả nào**. Nếu ra → dừng lại, đừng push.

---

## Những gì có trong thư mục này

| File | Nội dung |
|---|---|
| `patch_bots.py` | Script vá — chạy 1 lần ở bước 1 |
| `bot_v74.py` | Bản vá của `Quecoinnendkmoi--run.py` (do bước 1 sinh ra) |
| `bot_sentinel.py` | Bản vá của `Quetbankhongclaude--run.py` (do bước 1 sinh ra) |
| `.github/workflows/v74.yml` | Chạy V74 mỗi giờ trên GitHub Actions |
| `.github/workflows/sentinel.yml` | Chạy Sentinel mỗi giờ (lệch 20 phút so với V74) |
| `.github/workflows/keepalive.yml` | Ping Render 10 phút/lần cho khỏi ngủ |
| `main_render.py` | Chạy CẢ 2 bot + Flask API trong 1 tiến trình |
| `render.yaml` | Cấu hình deploy Render 1 chạm |
| `requirements.txt` | Thư viện cần cài |
| `.gitignore` | Chặn `.env` và ảnh chart lọt lên GitHub |
| `.env.example` | Mẫu biến môi trường để chạy máy nhà |

Thay đổi chính trong code:

- `TOKEN` / `CHAT_ID` giờ đọc từ biến môi trường, thiếu là báo lỗi rõ ràng rồi thoát
- Biến `RUN_ONCE=1` → quét 1 lần rồi thoát (cho GitHub Actions); `=0` → vòng lặp như cũ
- `EXCHANGE_ID` cho phép đổi sàn mà không sửa code (xử lý lỗi 451, xem phần 4)
- `matplotlib` chuyển sang backend `Agg` để vẽ chart được trên server không màn hình
- Sentinel lưu `prev_btc_level` ra file → tin "BTC HỒI PHỤC" vẫn bắn được dù mỗi lần chạy là process mới
- Vòng lặp Sentinel bọc `try/except` → lỗi mạng không làm chết bot (đây là lý do bot của bạn im từ 30/07)

---

## Phần 2 — Đẩy code lên GitHub (repo private)

```bash
cd "D:\Code python\Bot trade\deploy-github"

git init
git add .
git commit -m "Bot trade: chay tren GitHub Actions, token qua Secrets"
git branch -M main
```

Lên github.com → **New repository** → tên `bot-trade` → chọn **Private** → **Create** (đừng tick thêm README/gitignore).

```bash
git remote add origin https://github.com/<ten-github-cua-ban>/bot-trade.git
git push -u origin main
```

> **Kiểm tra bắt buộc:** sau khi push, mở repo trên web, bấm vào `bot_v74.py` và `bot_sentinel.py`, dùng Ctrl+F tìm chuỗi `:AAG`. Phải **không tìm thấy**.

---

## Phần 3 — Ẩn token bằng GitHub Secrets

Trong repo: **Settings** → **Secrets and variables** → **Actions**

**Tab Secrets** → *New repository secret*, tạo 2 cái:

| Name | Secret |
|---|---|
| `TELEGRAM_TOKEN` | token MỚI từ BotFather |
| `TELEGRAM_CHAT_ID` | `5794043991` |

**Tab Variables** → *New repository variable* (tuỳ chọn, để đổi sàn không cần sửa code):

| Name | Value |
|---|---|
| `EXCHANGE_ID` | `binance` |
| `BINANCE_DATA_MIRROR` | `0` |

Secrets được mã hoá, không ai xem lại được kể cả bạn, và GitHub tự che chúng thành `***` trong log.

---

## Phần 4 — Chạy thử

Tab **Actions** → chọn `Bot V74` → **Run workflow** → **Run workflow**.

Đợi 2-4 phút. Mở log xem dòng `[EX] San dang dung: binance`. Nếu Telegram nổ tin → xong.

Sau đó cron tự chạy mỗi giờ. Lưu ý về cron của GitHub:

- **Chạy trễ 5-20 phút là bình thường**, giờ cao điểm có thể trễ hơn hoặc bỏ lượt
- **Repo public → Actions KHÔNG giới hạn phút.** Chạy 1 giờ/lần thoải mái, không lo quota
- Repo private thì chỉ có 2.000 phút/tháng, hai bot 1h/lần sẽ vượt (~4.300 phút)

Vì repo để public, **code của bạn ai cũng đọc được** — nhưng token thì không, vì nó nằm trong Secrets. Cụ thể với repo public cần nhớ:

- Người khác fork repo và mở Pull Request **không** đọc được Secrets của bạn (GitHub chặn sẵn) — an toàn
- Toàn bộ tham số chiến thuật (`MIN_GROWTH_PCT`, `LAZY_RS_MAX`, ngưỡng squeeze...) sẽ công khai
- Log của Actions cũng công khai → đừng `print()` token hay thông tin riêng tư ra log
- `TELEGRAM_CHAT_ID` cũng nên để trong Secrets (đã làm sẵn) thay vì viết thẳng vào code

---

## Phần 5 — Lỗi 451 (Binance chặn IP)

Đây là rủi ro lớn nhất khi chạy trên GitHub Actions. Runner của GitHub đặt ở Mỹ, mà Binance chặn IP datacenter Mỹ với thông báo:

```
Service unavailable from a restricted location according to 'b. Eligibility'
HTTP 451
```

Nếu log báo lỗi này, thử lần lượt:

**Cách 1 — mirror dữ liệu công khai của Binance**
Đổi Variable `BINANCE_DATA_MIRROR` = `1`, chạy lại.

**Cách 2 — đổi sàn (khuyên dùng, gần như chắc chắn được)**
Đổi Variable `EXCHANGE_ID` sang `kucoin`, `okx`, `bybit`, hoặc `gateio`.
Hai bot chỉ đọc dữ liệu công khai (nến + ticker) nên đổi sàn là drop-in, không cần sửa dòng code nào. Lưu ý danh sách coin và thanh khoản mỗi sàn hơi khác nhau → kết quả quét sẽ lệch chút so với Binance.

**Cách 3 — deploy ở Singapore** → xem phần 6. Đây là cách duy nhất giữ nguyên 100% dữ liệu Binance.

---

## Phần 6 — Render (giữ được Flask API cho app Android) 🔥

Bạn đã chọn "cần API thật", mà GitHub Actions **không host server được** — job chạy xong là máy bị huỷ, không có URL cố định. Render là chỗ hợp lý nhất.

**Deploy:**

1. Vào [render.com](https://render.com) → đăng ký bằng tài khoản GitHub
2. **New** → **Blueprint** → chọn repo `bot-trade` → Render tự đọc `render.yaml`
3. Nó sẽ hỏi 2 biến `TELEGRAM_TOKEN` và `TELEGRAM_CHAT_ID` → nhập token mới vào đây
4. **Apply** → chờ build ~5 phút

Xong bạn có URL kiểu `https://bot-trade.onrender.com`:

- `https://bot-trade.onrender.com/signals` → JSON cho app Android
- `https://bot-trade.onrender.com/health` → dùng để ping

**Vì sao chọn region `singapore`** (đã set sẵn trong `render.yaml`): IP Singapore không bị Binance chặn, nên giữ nguyên được dữ liệu Binance.

**Chống ngủ:** gói free ngủ sau **15 phút** không có request, lần gọi đầu sau đó mất 30-60 giây để tỉnh. Hai cách giữ thức:

- Đặt Variable `RENDER_URL` = `https://bot-trade.onrender.com` trong GitHub → `keepalive.yml` tự ping 10 phút/lần (tốn ~150 phút Actions/tháng, không đáng kể)
- Hoặc dùng [cron-job.org](https://cron-job.org) (free) ping `/health` mỗi 10 phút

**Giới hạn free tier Render:** 512 MB RAM, 0.1 CPU, 750 giờ/tháng. Sentinel quét 300 cặp có thể chạm trần RAM — nếu log báo `Out of memory`, giảm `LIMIT_SCAN` từ 300 xuống 150 trong `bot_sentinel.py`.

---

## Phần 7 — Chạy ở máy nhà (vẫn ẩn được token)

```bash
copy .env.example .env
# mở .env, dán token mới vào

pip install -r requirements.txt python-dotenv
```

Thêm 2 dòng đầu file `bot_v74.py` và `bot_sentinel.py`:

```python
from dotenv import load_dotenv
load_dotenv()
```

Rồi `python main_render.py`. File `.env` đã nằm trong `.gitignore` nên không bao giờ lên GitHub.

---

## Nên chọn cái nào?

| | GitHub Actions (repo public) | Render free |
|---|---|---|
| Miễn phí | ✅ không giới hạn phút | 750 giờ/tháng |
| Đủ cho 2 bot chạy 1h/lần | ✅ | ✅ |
| Flask API cho app Android | ❌ không có | ✅ |
| Binance chặn IP | ⚠️ rất dễ dính 451, phải đổi sàn | ✅ region Singapore |
| Đúng giờ | trễ 5-20 phút | chính xác |
| Lộ code | ⚠️ public, ai cũng đọc được | ✅ repo có thể để private |
| Setup | dễ hơn | thêm ~5 phút |

**Với repo public, GitHub Actions là đủ** nếu bạn chỉ cần tin về Telegram. Làm theo phần 1→5 là xong.

**Khi nào cần thêm Render:** khi app Android của bạn phải gọi được `/signals`, hoặc khi bạn muốn giữ nguyên dữ liệu Binance mà không phải đổi sàn. Lúc đó dùng Render làm bản chạy chính, và xoá khối `schedule:` trong `v74.yml` + `sentinel.yml` (chỉ giữ `workflow_dispatch:`) để Actions thành nút bấm tay dự phòng.

---

## Khắc phục sự cố nhanh

| Triệu chứng | Nguyên nhân |
|---|---|
| `[FATAL] Thieu bien moi truong TELEGRAM_TOKEN` | Chưa tạo Secret, hoặc gõ sai tên (phân biệt hoa thường) |
| `HTTP 451` | Binance chặn IP → phần 4 |
| Telegram im lặng, log không lỗi | Token đã bị revoke, hoặc sai `CHAT_ID` |
| Cron không chạy | Repo private không hoạt động 60 ngày sẽ bị GitHub tự tắt schedule — vào Actions bấm *Enable* lại |
| `Out of memory` trên Render | Giảm `LIMIT_SCAN` xuống 150 |
| Ảnh chart không gửi được | Thiếu `matplotlib`/`mplfinance` trong `requirements.txt` |
