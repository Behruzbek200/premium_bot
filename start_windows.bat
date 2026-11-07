@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist ".env" (
  copy ".env.example" ".env" >nul
  echo .env yaratildi. Uni ochib BOT_TOKEN va OWNER_ID ni kiriting.
  notepad .env
  pause
  exit /b 0
)
python -m pip install -r requirements.txt
if errorlevel 1 (
  echo Kutubxonalar ornalmadi. Internetni tekshiring.
  pause
  exit /b 1
)
python -m py_compile bot.py
if errorlevel 1 (
  echo Syntax xatosi mavjud!
  pause
  exit /b 1
)
python bot.py
pause
