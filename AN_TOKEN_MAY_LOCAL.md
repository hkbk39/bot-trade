# Ẩn token khi chạy Python ở máy — công thức dùng cho mọi file

Nguyên tắc: **token không bao giờ nằm trong file `.py`**. Nó nằm ở nơi khác, code chỉ đi lấy.
Nhờ vậy bạn gửi mã nguồn cho ai cũng được, đăng lên GitHub cũng được — không lộ gì.

Có 2 cách. Cách 1 dùng cho từng dự án, cách 2 dùng cho cả máy.

---

## Cách 1 — File `.env` (khuyên dùng)

### Cài 1 lần

```powershell
pip install python-dotenv
```

### Tạo file `.env` cạnh file `.py`

Nội dung:

```
TELEGRAM_TOKEN=8123456789:AAExxxxxxxxxxxxxxxxxxxxxxxxxx
TELEGRAM_CHAT_ID=5794043991
```

Không dấu nháy, không khoảng trắng quanh dấu `=`.

### Sửa file `.py`

Thay dòng cũ:

```python
TOKEN   = "8123456789:AAExxxxx"      # ❌ lộ khi gửi code
CHAT_ID = "5794043991"
```

Thành:

```python
import os
from dotenv import load_dotenv

load_dotenv()                                  # đọc file .env

TOKEN   = os.environ["TELEGRAM_TOKEN"]         # ✅ code sạch
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
```

Muốn báo lỗi dễ hiểu hơn khi quên tạo `.env`:

```python
TOKEN = os.environ.get("TELEGRAM_TOKEN")
if not TOKEN:
    raise SystemExit("Thiếu TELEGRAM_TOKEN — tạo file .env cạnh file này.")
```

### Chặn `.env` lọt lên GitHub

Tạo file tên `.gitignore` cùng thư mục, nội dung:

```
.env
```

Xong. Từ giờ `git add .` sẽ tự bỏ qua `.env`.

### Khi gửi code cho người khác

Gửi kèm file `.env.example` (không chứa token thật) để họ biết cần điền gì:

```
TELEGRAM_TOKEN=dien_token_cua_ban_vao_day
TELEGRAM_CHAT_ID=dien_chat_id_vao_day
```

---

## Cách 2 — Biến môi trường Windows (không cần file nào)

Hợp khi bạn có **nhiều file bot dùng chung một token**. Khai báo 1 lần, mọi file đều lấy được.

Mở PowerShell, chạy:

```powershell
setx TELEGRAM_TOKEN "8123456789:AAExxxxxxxxxxxxxxxxxxxxxxxxxx"
setx TELEGRAM_CHAT_ID "5794043991"
```

**Phải đóng PowerShell và mở lại** thì biến mới có hiệu lực (`setx` chỉ áp dụng cho tiến trình mới).

Kiểm tra:

```powershell
echo $env:TELEGRAM_TOKEN
```

Trong code chỉ cần:

```python
import os
TOKEN   = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
```

Không cần `python-dotenv`, không cần file `.env`.

Muốn xoá biến sau này:

```powershell
reg delete "HKCU\Environment" /v TELEGRAM_TOKEN /f
```

---

## Nên chọn cách nào?

| | `.env` | Biến môi trường Windows |
|---|---|---|
| Mỗi dự án một token khác nhau | ✅ | ❌ dùng chung |
| Chép dự án sang máy khác | ✅ mang theo `.env` | ❌ phải `setx` lại |
| Nguy cơ vô ý push lên GitHub | ⚠️ có (nhớ `.gitignore`) | ✅ không có file để push |
| Cần cài thêm thư viện | có | không |

Thực tế nên **dùng cả hai**: `setx` cho token chung, `.env` cho dự án nào cần token riêng. Code viết `os.environ.get(...)` là chạy được với cả hai.

---

## Riêng 2 bot trong thư mục này

`patch_bots.py` đã tự thêm sẵn phần đọc `.env` vào `bot_v74.py` và `bot_sentinel.py`. Bạn chỉ cần:

```powershell
cd "D:\Code python\Bot trade\deploy-github"
pip install -r requirements.txt
copy .env.example .env
notepad .env
```

Điền token mới vào rồi lưu. Chạy:

```powershell
python bot_v74.py          # chỉ bot V74, kèm Flask API port 5000
python bot_sentinel.py     # chỉ bot Sentinel
python main_render.py      # cả 2 bot + Flask API cùng lúc
```

Hoặc nhấp đúp `chay_local.bat`.

Muốn quét đúng 1 lần rồi thoát (để test nhanh):

```powershell
$env:RUN_ONCE="1"; python bot_sentinel.py
```

---

## Kiểm tra trước khi gửi code cho ai đó

```powershell
findstr /S /C:":AAG" *.py
```

Không ra kết quả nào là an toàn. Ra kết quả → file đó còn token, sửa trước khi gửi.
