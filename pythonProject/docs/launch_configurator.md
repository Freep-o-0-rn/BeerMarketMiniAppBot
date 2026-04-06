# Мини-конфигуратор запуска

Для централизованной настройки запусков используются:

- `launch_configurator.bat` — интерактивный мастер настройки.
- `launch_config.py` — CLI-инструмент для чтения/записи конфигурации.
- `settings/launch_config.json` — файл значений по умолчанию.

## Что настраивается

### `start_bot.bat`
- `entry` — входной Python-файл (по умолчанию `main.py`).
- `requirements` — файл зависимостей для режима `/setup`.
- `setup_by_default` — запускать установку зависимостей без аргумента `/setup`.

### `start_cloudflare_stack.bat`
- `api_port` — порт API по умолчанию.
- `web_port` — порт webapp по умолчанию.
- - `api_host` — bind-host API (`127.0.0.1` только локально, `0.0.0.0` для внешнего доступа).
- `web_host` — bind-host webapp (`127.0.0.1` только локально, `0.0.0.0` для внешнего доступа).
- `open_firewall_ports` — автоматически создать inbound-правила Windows Firewall для `api_port` и `web_port`.
- `public_api_url` — публичный URL API для вывода в подсказке.
- `public_app_url` — публичный URL webapp для вывода в подсказке.

Дополнительно, если `cloudflared` установлен и доступен в `PATH`, `start_cloudflare_stack.bat`
автоматически поднимает **два Cloudflare Quick Tunnel** (для API и webapp) и выводит
временные публичные `*.trycloudflare.com` URL для быстрой проверки доступа из интернета.

## Приоритет источников

### `start_bot.bat`
1. Аргумент командной строки (`/setup`).
2. `settings/launch_config.json`.
3. Встроенные дефолты батника.

### `start_cloudflare_stack.bat`
1. Аргументы командной строки (`/api_port:NNNN`, `/web_port:NNNN`, `/api_host:HOST`, `/web_host:HOST`, `/open_ports`, `/setup`).
2. Переменные окружения (`NEWS_API_PORT`, `WEBAPP_PORT`, `NEWS_API_HOST`, `WEBAPP_HOST`, `OPEN_FIREWALL_PORTS`).
3. `settings/launch_config.json`.
4. Встроенные дефолты батника.

## CLI команды

```bash
python launch_config.py configure
python launch_config.py show
python launch_config.py reset
python launch_config.py export-bat start_bot
python launch_config.py export-bat start_cloudflare_stack
```