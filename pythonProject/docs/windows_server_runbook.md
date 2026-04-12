\# Windows Server Runbook (без изменения кода)



Этот проект можно поднять на Windows 11 Pro \*\*без правок исходников\*\*. Чаще всего проблема после переноса — окружение, а не код.



\## 1) Проверить Python и pip



```bat

py -3 --version

py -3 -m pip --version

```



Если `py` недоступен:



```bat

python --version

python -m pip --version

```



\## 2) Установить зависимости проекта



Из каталога `pythonProject`:



```bat

py -3 -m pip install -r requirements.txt

```



\## 3) Запустить стек штатным скриптом



```bat

start\_cloudflare\_stack.bat /setup

```



> Флаг `/setup` внутри скрипта повторно установит зависимости и безопасен для повторных запусков.



\## 4) Проверить локальные эндпоинты на сервере



```bat

curl http://localhost:8081/health

curl http://localhost:8080/

```



Ожидается:

\- `8081/health` возвращает JSON с `ok: true`.

\- `8080/` отдает index webapp.



\## 5) Проверить доступ с другой машины в сети



```bat

curl http://<IP\_СЕРВЕРА>:8081/health

curl http://<IP\_СЕРВЕРА>:8080/

```



Если локально работает, а удаленно нет — причина обычно в firewall/NAT.



\## 6) Открыть порты в Windows Defender Firewall (если нужно)



Запустить CMD/PowerShell от администратора:



```bat

netsh advfirewall firewall add rule name="BeerMarket API 8081" dir=in action=allow protocol=TCP localport=8081

netsh advfirewall firewall add rule name="BeerMarket WebApp 8080" dir=in action=allow protocol=TCP localport=8080

```



\## 7) Быстрая диагностика занятых портов



```bat

netstat -ano | findstr :8081

netstat -ano | findstr :8080

```



Если порт занят посторонним процессом:



```bat

taskkill /PID <PID> /F

```



\## 8) Типовые причины, почему «не запускается»



1\. Не установлены зависимости (`aiohttp` и др.).

2\. Неправильная рабочая директория (скрипт запущен не из `pythonProject`).

3\. Порты 8080/8081 заняты другими процессами.

4\. Входящие порты закрыты firewall или правилами на роутере/VPS.



\## 9) Что НЕ требуется менять в коде



\- API уже запускается на `0.0.0.0:8081`, это корректно для внешнего доступа.

\- Для стандартного сценария миграции достаточно настроить окружение и сеть.



