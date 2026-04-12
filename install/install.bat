@echo off
setlocal EnableExtensions

:: --- Переходим в папку, где лежит сам bat ---
cd /d "%~dp0"

:: --- Проверка прав администратора ---
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Требуются права администратора. Запрашиваю UAC...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath '%~f0' -Verb RunAs"
    exit /b
)

set "MSI=cloudflared-windows-amd64.msi"
set "LOG=%~dp0cloudflared-install.log"

if not exist "%MSI%" (
    echo [ERROR] Файл "%MSI%" не найден в папке:
    echo %~dp0
    pause
    exit /b 1
)

echo [INFO] Устанавливаю Cloudflared...
msiexec /i "%MSI%" /qn /norestart /L*v "%LOG%"
set "RC=%errorlevel%"

if not "%RC%"=="0" (
    echo [ERROR] Установка завершилась с кодом %RC%
    echo [INFO] Смотри лог: "%LOG%"
    pause
    exit /b %RC%
)

echo [OK] Установка завершена успешно.
echo [INFO] Проверяю версию...
cloudflared --version
if %errorlevel% neq 0 (
    echo [WARN] cloudflared не найден в PATH в этой сессии.
    echo [INFO] Закрой и заново открой консоль.
)

echo.
echo [DONE] Можно запускать start_cloudflare_stack.bat
pause
exit /b 0