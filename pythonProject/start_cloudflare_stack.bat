@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION

rem ==========================================================
rem BeerMarket Mini App stack launcher (Windows)
rem - Starts API and static web app on configurable host/ports
rem - Optionally opens inbound Windows Firewall rules
rem - Checks local health endpoints
rem ==========================================================

cd /d "%~dp0"
set "ROOT_DIR=%~dp0"
if "%ROOT_DIR:~-1%"=="\" set "ROOT_DIR=%ROOT_DIR:~0,-1%"

title BeerMarket MiniApp Stack Launcher

echo ==========================================================
echo   BeerMarket Mini App: запуск локального стека
echo ==========================================================
echo.

rem --- Defaults (can be overridden by config/env/CLI) ---
set "API_PORT=8091"
set "WEB_PORT=8090"
set "API_HOST=127.0.0.1"
set "WEB_HOST=127.0.0.1"
set "OPEN_FIREWALL_PORTS=0"
set "PUBLIC_API_URL=https://api.freep0rndeveloper.website/"
set "PUBLIC_APP_URL=https://app.freep0rndeveloper.website/"
set "DO_SETUP=0"

rem --- Find Python executable early (needed for config import) ---

set "PYTHON_EXE="
set "PYTHON_ARGS="
if not defined PYTHON_EXE if exist ".venv\Scripts\python.exe" set "PYTHON_EXE=.venv\Scripts\python.exe"
if not defined PYTHON_EXE if exist "venv\Scripts\python.exe" set "PYTHON_EXE=venv\Scripts\python.exe"
if not defined PYTHON_EXE (
  where py >nul 2>&1 && (set "PYTHON_EXE=py" & set "PYTHON_ARGS=-3")
)
if not defined PYTHON_EXE (
  where python >nul 2>&1 && (set "PYTHON_EXE=python")
)
if not defined PYTHON_EXE (
  echo [ERROR] Python не найден. Установи Python 3.8+ или активируй venv.
  pause
  exit /b 1
)

echo [INFO] Python: %PYTHON_EXE% %PYTHON_ARGS%

rem --- Load launch config (defaults layer) ---
for /f "usebackq tokens=1,* delims==" %%A in (`%PYTHON_EXE% %PYTHON_ARGS% launch_config.py export-bat start_cloudflare_stack 2^>nul`) do (
  if /I "%%A"=="CFG_API_PORT" set "API_PORT=%%B"
  if /I "%%A"=="CFG_WEB_PORT" set "WEB_PORT=%%B"
  if /I "%%A"=="CFG_API_HOST" set "API_HOST=%%B"
  if /I "%%A"=="CFG_WEB_HOST" set "WEB_HOST=%%B"
  if /I "%%A"=="CFG_OPEN_FIREWALL_PORTS" set "OPEN_FIREWALL_PORTS=%%B"
  if /I "%%A"=="PUBLIC_API_URL" set "PUBLIC_API_URL=%%B"
  if /I "%%A"=="PUBLIC_APP_URL" set "PUBLIC_APP_URL=%%B"
)

rem --- Environment layer ---
if defined NEWS_API_PORT set "API_PORT=%NEWS_API_PORT%"
if defined WEBAPP_PORT set "WEB_PORT=%WEBAPP_PORT%"
if defined NEWS_API_HOST set "API_HOST=%NEWS_API_HOST%"
if defined WEBAPP_HOST set "WEB_HOST=%WEBAPP_HOST%"
if defined OPEN_FIREWALL_PORTS set "OPEN_FIREWALL_PORTS=%OPEN_FIREWALL_PORTS%"

rem --- CLI layer ---
for %%I in (%*) do (
  set "ARG=%%~I"
  if /I "!ARG!"=="/setup" set "DO_SETUP=1"
  if /I "!ARG:~0,10!"=="/api_port:" set "API_PORT=!ARG:~10!"
  if /I "!ARG:~0,10!"=="/web_port:" set "WEB_PORT=!ARG:~10!"
  if /I "!ARG:~0,10!"=="/api_host:" set "API_HOST=!ARG:~10!"
  if /I "!ARG:~0,10!"=="/web_host:" set "WEB_HOST=!ARG:~10!"
  if /I "!ARG!"=="/open_ports" set "OPEN_FIREWALL_PORTS=1"
)

echo [INFO] API bind: %API_HOST%:%API_PORT%
echo [INFO] WEB bind: %WEB_HOST%:%WEB_PORT%

rem --- Restart mode: stop old listeners on target ports ---
echo [STEP] Проверяю и останавливаю старые процессы на портах %API_PORT% и %WEB_PORT%...
for %%P in (%API_PORT% %WEB_PORT%) do (
  for /f "tokens=5" %%a in ('netstat -ano ^| findstr /r /c:":%%P .*LISTENING"') do (
    echo [INFO] Останавливаю PID %%a (порт %%P)
    taskkill /PID %%a /F >nul 2>&1
  )
)
timeout /t 1 /nobreak >nul

rem --- Optional dependency install ---
if "%DO_SETUP%"=="1" (
  if exist "requirements.txt" (
    echo [INFO] Установка зависимостей из requirements.txt ...
    %PYTHON_EXE% %PYTHON_ARGS% -m pip install -r requirements.txt
  ) else (
    echo [WARN] requirements.txt не найден, пропускаю установку.
  )
)

rem --- Optional Windows Firewall open ---
if "%OPEN_FIREWALL_PORTS%"=="1" (
  echo [STEP] Открываю входящие порты в Windows Firewall...
  netsh advfirewall firewall delete rule name="BeerMarket API %API_PORT%" >nul 2>&1
  netsh advfirewall firewall delete rule name="BeerMarket Web %WEB_PORT%" >nul 2>&1
  netsh advfirewall firewall add rule name="BeerMarket API %API_PORT%" dir=in action=allow protocol=TCP localport=%API_PORT% >nul 2>&1
  if errorlevel 1 (
    echo [WARN] Не удалось открыть порт API %API_PORT% (нужны права администратора).
  ) else (
    echo [OK] Firewall rule создана для API порта %API_PORT%.
  )
  netsh advfirewall firewall add rule name="BeerMarket Web %WEB_PORT%" dir=in action=allow protocol=TCP localport=%WEB_PORT% >nul 2>&1
  if errorlevel 1 (
    echo [WARN] Не удалось открыть порт Web %WEB_PORT% (нужны права администратора).
  ) else (
    echo [OK] Firewall rule создана для Web порта %WEB_PORT%.
  )
)

rem --- Start API in dedicated window ---
echo [STEP] Запускаю API: http://%API_HOST%:%API_PORT%
set "API_CMD=cd /d ""%ROOT_DIR%"" && set NEWS_API_PORT=%API_PORT% && set NEWS_API_HOST=%API_HOST% && %PYTHON_EXE% %PYTHON_ARGS% -m api.app"
start "BeerMarket API :%API_PORT%" cmd /k "%API_CMD%"

rem --- Start WebApp in dedicated window ---
echo [STEP] Запускаю WebApp: http://%WEB_HOST%:%WEB_PORT%
if not exist "%ROOT_DIR%\webapp" (
  echo [ERROR] Не найдена директория webapp: %ROOT_DIR%\webapp
  echo [HINT] Проверь структуру проекта рядом со start_cloudflare_stack.bat
  pause
  exit /b 1
)
set "WEB_CMD=cd /d ""%ROOT_DIR%\webapp"" && %PYTHON_EXE% %PYTHON_ARGS% -m http.server %WEB_PORT% --bind %WEB_HOST%"
start "BeerMarket WebApp :%WEB_PORT%" cmd /k "%WEB_CMD%"

echo.
echo [INFO] Ожидаю запуск сервисов (6 сек)...
timeout /t 6 /nobreak >nul

rem --- Health checks from localhost ---
set "CHECK_OK=1"

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing http://127.0.0.1:%API_PORT%/health -TimeoutSec 5; if ($r.StatusCode -eq 200) { exit 0 } else { exit 1 } } catch { exit 1 }"
if errorlevel 1 (
  echo [WARN] API health-check не пройден: http://127.0.0.1:%API_PORT%/health
  set "CHECK_OK=0"
) else (
  echo [OK] API health-check: http://127.0.0.1:%API_PORT%/health
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing http://127.0.0.1:%WEB_PORT%/ -TimeoutSec 5; if ($r.StatusCode -eq 200) { exit 0 } else { exit 1 } } catch { exit 1 }"
if errorlevel 1 (
  echo [WARN] WebApp check не пройден: http://127.0.0.1:%WEB_PORT%/
  set "CHECK_OK=0"
) else (
  echo [OK] WebApp check: http://127.0.0.1:%WEB_PORT%/
)

echo.
echo ===================== ПУБЛИЧНЫЕ КОРРЕКТНЫЕ URL =====================
echo API: https://api.freep0rndeveloper.website/
echo APP: https://app.freep0rndeveloper.website/
echo ===================== ПУБЛИЧНЫЕ ТЕКУЩИЕ URL =====================
echo API: %PUBLIC_API_URL%
echo APP: %PUBLIC_APP_URL%
echo.
echo Рекомендуемый Main App URL в BotFather:
echo https://app.freep0rndeveloper.website/?api_base=https://api.freep0rndeveloper.website/
echo Текущий Main App URL в BotFather:
echo %PUBLIC_APP_URL%?api_base=%PUBLIC_API_URL%
echo ==========================================================
echo.

if "%CHECK_OK%"=="1" (
  echo [SUCCESS] Локальный стек запущен и отвечает.
) else (
  echo [WARN] Один или несколько локальных checks не пройдены.
  echo        Проверь отдельные окна API/WebApp на ошибки.
)

echo.
echo Нажми любую клавишу, чтобы закрыть это окно.
pause >nul
exit /b 0
