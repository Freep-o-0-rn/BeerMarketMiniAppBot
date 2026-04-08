from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "settings" / "launch_config.json"

DEFAULTS = {
    "start_bot": {
        "entry": "main.py",
        "requirements": "requirements.txt",
        "setup_by_default": False,
    },
    "start_cloudflare_stack": {
        "api_port": 8091,
        "web_port": 8090,
        "api_host": "127.0.0.1",
        "web_host": "127.0.0.1",
        "open_firewall_ports": False,
        "public_api_url": "https://api.freep0rndeveloper.website/",
        "public_app_url": "https://app.freep0rndeveloper.website/",
    },
}


def _ensure_parent() -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)


def _normalize(data: dict) -> dict:
    cfg = {
        "start_bot": dict(DEFAULTS["start_bot"]),
        "start_cloudflare_stack": dict(DEFAULTS["start_cloudflare_stack"]),
    }
    if not isinstance(data, dict):
        return cfg

    sb = data.get("start_bot")
    if isinstance(sb, dict):
        if isinstance(sb.get("entry"), str) and sb["entry"].strip():
            cfg["start_bot"]["entry"] = sb["entry"].strip()
        if isinstance(sb.get("requirements"), str) and sb["requirements"].strip():
            cfg["start_bot"]["requirements"] = sb["requirements"].strip()
        if isinstance(sb.get("setup_by_default"), bool):
            cfg["start_bot"]["setup_by_default"] = sb["setup_by_default"]

    scs = data.get("start_cloudflare_stack")
    if isinstance(scs, dict):
        for key in ("api_port", "web_port"):
            value = scs.get(key)
            if isinstance(value, int) and 1 <= value <= 65535:
                cfg["start_cloudflare_stack"][key] = value
        for key in ("api_host", "web_host"):
            value = scs.get(key)
            if isinstance(value, str) and value.strip():
                cfg["start_cloudflare_stack"][key] = value.strip()
        if isinstance(scs.get("open_firewall_ports"), bool):
            cfg["start_cloudflare_stack"]["open_firewall_ports"] = scs["open_firewall_ports"]
        for key in ("public_api_url", "public_app_url"):
            value = scs.get(key)
            if isinstance(value, str) and value.strip():
                cfg["start_cloudflare_stack"][key] = value.strip()

    return cfg


def read_config() -> dict:
    try:
        if CONFIG_PATH.exists():
            with CONFIG_PATH.open("r", encoding="utf-8") as f:
                return _normalize(json.load(f))
    except Exception:
        pass
    return _normalize({})


def write_config(cfg: dict) -> None:
    _ensure_parent()
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(_normalize(cfg), f, ensure_ascii=False, indent=2)


def ensure_config_file() -> dict:
    cfg = read_config()
    if not CONFIG_PATH.exists():
        write_config(cfg)
    return cfg


def _to_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "y", "on", "да"}


def _prompt(prompt: str, current: str) -> str:
    entered = input(f"{prompt} [{current}]: ").strip()
    return entered if entered else current


def interactive_configure() -> int:
    cfg = ensure_config_file()

    print("=== Мини-конфигуратор запуска BeerMarket ===")
    print(f"Файл конфигурации: {CONFIG_PATH}")
    print("Оставь поле пустым, чтобы оставить текущее значение.\n")

    sb = cfg["start_bot"]
    scs = cfg["start_cloudflare_stack"]

    sb["entry"] = _prompt("start_bot: файл входа", str(sb["entry"]))
    sb["requirements"] = _prompt("start_bot: файл зависимостей", str(sb["requirements"]))
    sb_setup = _prompt("start_bot: делать pip install по умолчанию? (true/false)", str(sb["setup_by_default"]).lower())
    sb["setup_by_default"] = _to_bool(sb_setup)

    while True:
        api_port_raw = _prompt("start_cloudflare_stack: API порт", str(scs["api_port"]))
        web_port_raw = _prompt("start_cloudflare_stack: Web порт", str(scs["web_port"]))
        try:
            api_port = int(api_port_raw)
            web_port = int(web_port_raw)
            if not (1 <= api_port <= 65535 and 1 <= web_port <= 65535):
                raise ValueError
            scs["api_port"] = api_port
            scs["web_port"] = web_port
            break
        except ValueError:
            print("[WARN] Порты должны быть числами от 1 до 65535. Повтори ввод.")

    scs["api_host"] = _prompt("start_cloudflare_stack: API host (127.0.0.1 или 0.0.0.0)", str(scs["api_host"]))
    scs["web_host"] = _prompt("start_cloudflare_stack: Web host (127.0.0.1 или 0.0.0.0)", str(scs["web_host"]))
    scs_open_fw = _prompt(
        "start_cloudflare_stack: открыть порты в Windows Firewall? (true/false)",
        str(scs["open_firewall_ports"]).lower(),
    )
    scs["open_firewall_ports"] = _to_bool(scs_open_fw)
    scs["public_api_url"] = _prompt("start_cloudflare_stack: публичный API URL", str(scs["public_api_url"]))
    scs["public_app_url"] = _prompt("start_cloudflare_stack: публичный APP URL", str(scs["public_app_url"]))

    write_config(cfg)
    print("\n[OK] Конфигурация сохранена.")
    return 0


def export_bat(target: str) -> int:
    cfg = ensure_config_file()
    if target == "start_bot":
        sb = cfg["start_bot"]
        print(f"ENTRY={sb['entry']}")
        print(f"REQS={sb['requirements']}")
        print(f"SETUP_BY_DEFAULT={1 if sb['setup_by_default'] else 0}")
        return 0

    if target == "start_cloudflare_stack":
        scs = cfg["start_cloudflare_stack"]
        print(f"CFG_API_PORT={scs['api_port']}")
        print(f"CFG_WEB_PORT={scs['web_port']}")
        print(f"CFG_API_HOST={scs['api_host']}")
        print(f"CFG_WEB_HOST={scs['web_host']}")
        print(f"CFG_OPEN_FIREWALL_PORTS={1 if scs['open_firewall_ports'] else 0}")
        print(f"PUBLIC_API_URL={scs['public_api_url']}")
        print(f"PUBLIC_APP_URL={scs['public_app_url']}")
        return 0

    print(f"Unknown export target: {target}", file=sys.stderr)
    return 2


def show() -> int:
    cfg = ensure_config_file()
    print(json.dumps(cfg, ensure_ascii=False, indent=2))
    return 0


def reset() -> int:
    write_config(DEFAULTS)
    print(f"Reset done: {CONFIG_PATH}")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        return interactive_configure()

    cmd = argv[1].lower()
    if cmd == "configure":
        return interactive_configure()
    if cmd == "show":
        return show()
    if cmd == "reset":
        return reset()
    if cmd == "export-bat" and len(argv) >= 3:
        return export_bat(argv[2])

    print("Usage:")
    print("  python launch_config.py configure")
    print("  python launch_config.py show")
    print("  python launch_config.py reset")
    print("  python launch_config.py export-bat <start_bot|start_cloudflare_stack>")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))