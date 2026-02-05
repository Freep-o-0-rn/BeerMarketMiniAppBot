@echo off
chcp 65001 >nul
title BeerMarket Bot v0.0115
setlocal ENABLEDELAYEDEXPANSION

rem === Переход в папку, где лежит батник ===
cd /d "%~dp0"

rem === Настройки ===
set "ENTRY=main.py"
set "REQS=requirements.txt"

rem === Проверяем, существует ли main.py ===
if not exist "%ENTRY%" (
    echo [ERROR] main.py не найден в каталоге:
    echo    %~dp0
    pause
    exit /b 1
)

rem === Находим Python: venv -> py -> python ===
set "PYEXE="
if exist "venv\Scripts\python.exe" set "PYEXE=venv\Scripts\python.exe"
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

rem === Опционально: установка зависимостей ===
if /i "%~1"=="/setup" (
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
