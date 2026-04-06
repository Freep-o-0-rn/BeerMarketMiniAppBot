@echo off
chcp 65001 >nul
title BeerMarket Bot v0.96
setlocal ENABLEDELAYEDEXPANSION

rem === Переход в папку, где лежит батник ===
cd /d "%~dp0"

rem === Настройки (будут переопределены launch_config.py при наличии) ===
set "ENTRY=main.py"
set "REQS=requirements.txt"
set "SETUP_BY_DEFAULT=0"
set "DO_SETUP=0"

rem === Находим Python: venv -> .venv -> py -> python ===
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

rem === Подтягиваем дефолты запуска из settings/launch_config.json ===
if exist "launch_config.py" (
  for /f "usebackq tokens=1,* delims==" %%A in (`%PYEXE% launch_config.py export-bat start_bot`) do (
    set "%%A=%%B"
  )
)

rem === Переопределения через аргументы ===
if /i "%~1"=="/setup" set "DO_SETUP=1"

if "%DO_SETUP%"=="0" if "%SETUP_BY_DEFAULT%"=="1" set "DO_SETUP=1"

rem === Проверяем, существует ли ENTRY ===
if not exist "%ENTRY%" (
    echo [ERROR] %ENTRY% не найден в каталоге:
    echo    %~dp0
    pause
    exit /b 1
)

rem === Опционально: установка зависимостей ===
if "%DO_SETUP%"=="1" (
  if exist "%REQS%" (
    echo [INFO] Установка зависимостей из %REQS% ...
    %PYEXE% -m pip install -r "%REQS%"
  ) else (
    echo [WARN] %REQS% не найден, пропускаю установку.
  )
)

rem === Запуск ===
echo [INFO] Запуск: %PYEXE% "%ENTRY%"
%PYEXE% "%ENTRY%"
set "RC=%ERRORLEVEL%"

echo.
echo [INFO] Завершено с кодом %RC%.
pause
exit /b %RC%