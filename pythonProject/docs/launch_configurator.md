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
- `public_api_url` — публичный URL API для вывода в подсказке.
- `public_app_url` — публичный URL webapp для вывода в подсказке.

## Приоритет источников

### `start_bot.bat`
1. Аргумент командной строки (`/setup`).
2. `settings/launch_config.json`.
3. Встроенные дефолты батника.

### `start_cloudflare_stack.bat`
1. Аргументы командной строки (`/api_port:NNNN`, `/web_port:NNNN`, `/setup`).
2. Переменные окружения (`NEWS_API_PORT`, `WEBAPP_PORT`).
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