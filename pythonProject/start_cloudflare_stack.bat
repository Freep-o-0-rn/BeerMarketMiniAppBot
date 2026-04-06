@echo off
chcp 65001 >nul
setlocal ENABLEDELAYEDEXPANSION

rem ==========================================================
rem BeerMarket Mini App stack launcher (Windows)
rem - Starts API on localhost:8081
rem - Starts static web app on localhost:8080
rem - Checks local health endpoints
rem ==========================================================

cd /d "%~dp0"

title BeerMarket MiniApp Stack Launcher

rem --- Built-in defaults ---
set "API_PORT=8081"
set "WEB_PORT=8080"
set "PUBLIC_API_URL=https://api.freep0rndeveloper.website/"
set "PUBLIC_APP_URL=https://app.freep0rndeveloper.website/"
set "DO_SETUP=0"

rem --- Find Python executable ---
set "PYEXE="
if exist "venv\Scripts\python.exe" set "PYEXE=venv\Scripts\python.exe"
if not defined PYEXE if exist ".venv\Scripts\python.exe" set "PYEXE=.venv\Scripts\python.exe"
if not defined PYEXE (
  where py >nul 2>&1 && (set "PYEXE=py -3")
)
if not defined PYEXE (
  where python >nul 2>&1 && (set "PYEXE=python")
)
if not defined PYEXE (
  echo [ERROR] Python не найден. Установи Python 3.8+ или активируй venv.
  pause
  exit /b 1
)

rem --- Load defaults from settings/launch_config.json ---
if exist "launch_config.py" (
  for /f "usebackq tokens=1,* delims==" %%A in (`%PYEXE% launch_config.py export-bat start_cloudflare_stack`) do (
    set "%%A=%%B"
  )
  if defined CFG_API_PORT set "API_PORT=!CFG_API_PORT!"
  if defined CFG_WEB_PORT set "WEB_PORT=!CFG_WEB_PORT!"
)

rem --- ENV overrides (backward compatible) ---
if defined NEWS_API_PORT set "API_PORT=%NEWS_API_PORT%"
if defined WEBAPP_PORT set "WEB_PORT=%WEBAPP_PORT%"

rem Supported args:
rem   /setup
rem   /api_port:NNNN
rem   /web_port:NNNN
:parse_args
if "%~1"=="" goto args_done
if /i "%~1"=="/setup" (
  set "DO_SETUP=1"
) else if /i "%~1:~0,10%"=="/api_port:" (
  set "API_PORT=%~1:~10%"
) else if /i "%~1:~0,10%"=="/web_port:" (
  set "WEB_PORT=%~1:~10%"
)
shift
goto parse_args
:args_done

echo ==========================================================
echo   BeerMarket Mini App: запуск локального стека
echo ==========================================================
echo.
echo [INFO] API_PORT=%API_PORT%, WEB_PORT=%WEB_PORT%
echo.
echo [INFO] Python: %PYEXE%
echo.

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
    %PYEXE% -m pip install -r requirements.txt
  ) else (
    echo [WARN] requirements.txt не найден, пропускаю установку.
  )
)

rem --- Start API in dedicated window ---
echo [STEP] Запускаю API: http://localhost:%API_PORT%
start "BeerMarket API :%API_PORT%" cmd /k "cd /d "%~dp0" && set NEWS_API_PORT=%API_PORT% && %PYEXE% -m api.app"

rem --- Start WebApp in dedicated window ---
rem NOTE: явный bind на 127.0.0.1 обходит проблему WinError 10013 на части Windows-серверов,
rem где bind на IPv6 any (::) может быть запрещён политиками/резервами.
echo [STEP] Запускаю WebApp: http://localhost:%WEB_PORT%
start "BeerMarket WebApp :%WEB_PORT%" cmd /k "cd /d "%~dp0webapp" && %PYEXE% -m http.server %WEB_PORT% --bind 127.0.0.1"

echo.
echo [INFO] Ожидаю запуск сервисов (6 сек)...
timeout /t 6 /nobreak >nul

rem --- Health checks ---
set "CHECK_OK=1"

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing http://localhost:%API_PORT%/health -TimeoutSec 5; if ($r.StatusCode -eq 200) { exit 0 } else { exit 1 } } catch { exit 1 }"
if errorlevel 1 (
  echo [WARN] API health-check не пройден: http://localhost:%API_PORT%/health
  set "CHECK_OK=0"
) else (
  echo [OK] API health-check: http://localhost:%API_PORT%/health
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing http://localhost:%WEB_PORT%/ -TimeoutSec 5; if ($r.StatusCode -eq 200) { exit 0 } else { exit 1 } } catch { exit 1 }"
if errorlevel 1 (
  echo [WARN] WebApp check не пройден: http://localhost:%WEB_PORT%/
  set "CHECK_OK=0"
) else (
  echo [OK] WebApp check: http://localhost:%WEB_PORT%/
)

echo.
echo ===================== ПУБЛИЧНЫЕ URL =====================
echo API: %PUBLIC_API_URL%
echo APP: %PUBLIC_APP_URL%
echo.
echo Рекомендуемый Main App URL в BotFather:
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