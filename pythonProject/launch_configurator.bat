@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"
title BeerMarket Launch Configurator

set "PYEXE="
if exist "venv\Scripts\python.exe" set "PYEXE=venv\Scripts\python.exe"
if not defined PYEXE if exist ".venv\Scripts\python.exe" set "PYEXE=.venv\Scripts\python.exe"
if not defined PYEXE (
  where py >nul 2>&1 && set "PYEXE=py -3"
)
if not defined PYEXE (
  where python >nul 2>&1 && set "PYEXE=python"
)
if not defined PYEXE (
  echo [ERROR] Python не найден. Установи Python 3.8+ или активируй venv.
  pause
  exit /b 1
)

echo [INFO] Запускаю мини-конфигуратор через: %PYEXE%
%PYEXE% launch_config.py configure
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (
  echo [OK] Настройки запуска обновлены.
) else (
  echo [ERROR] Конфигуратор завершился с кодом %RC%.
)

pause
exit /b %RC%