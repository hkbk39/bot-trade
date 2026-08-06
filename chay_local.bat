@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".env" (
    echo.
    echo  [!] Chua co file .env
    echo      Chay:  copy .env.example .env
    echo      Roi mo .env dien token moi tu BotFather vao.
    echo.
    pause
    exit /b 1
)

echo ============================================
echo   BOT TRADE - CHAY O MAY LOCAL
echo   Token doc tu file .env (khong nam trong code)
echo   Nhan Ctrl+C de dung
echo ============================================
echo.

python main_render.py

echo.
echo  Bot da dung.
pause
