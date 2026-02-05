# bootstrap_env.py
# Создаёт/чинит .venv и ставит зависимости. Без shlex, только списки args.

import os
import sys
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
VENV = HERE / ".venv"
IS_WIN = os.name == "nt"
VENV_PY = VENV / ("Scripts/python.exe" if IS_WIN else "bin/python")

def run(argv, check=True):
    print(">", " ".join(map(str, argv)))
    return subprocess.run(list(map(str, argv))),  # not using check to keep вывод
    # Если хочешь падать на ошибке: return subprocess.run(argv, check=check)

def ensure_venv():
    if not VENV_PY.exists():
        print("Создаю виртуальное окружение:", VENV)
        subprocess.run([sys.executable, "-m", "venv", str(VENV)], check=True)
    else:
        print("Найдено существующее окружение:", VENV_PY)

def pyver(python):
    out = subprocess.check_output([str(python), "-c",
                                   "import sys;print(f'{sys.version_info.major}.{sys.version_info.minor}')"],
                                  text=True).strip()
    major, minor = map(int, out.split("."))
    return major, minor

def install_deps():
    # Если есть requirements.txt — используем его.
    req_file = HERE / "requirements.txt"
    if req_file.exists():
        print("Нашёл requirements.txt — устанавливаю по нему.")
        subprocess.run([str(VENV_PY), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], check=True)
        subprocess.run([str(VENV_PY), "-m", "pip", "install", "-r", str(req_file)], check=True)
        return

    # Иначе — дефолтный набор с учётом версии Python.
    deps = [
        "aiogram>=3.4,<4",
        "openpyxl>=3.1.2,<3.2",
        "xlrd==2.0.1",
        'pywin32>=306; platform_system == "Windows"',
        # numpy намеренно НЕ пиную — его подтянет совместимый от pandas
    ]

    maj, minr = pyver(VENV_PY)
    if maj >= 3 and minr >= 13:
        deps.append("pandas==2.3.3")
    else:
        deps.append("pandas==2.0.3")

    subprocess.run([str(VENV_PY), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"], check=True)
    subprocess.run([str(VENV_PY), "-m", "pip", "install", *deps], check=True)

def show_versions():
    code = r"""
import sys
print("Python:", sys.version)
def pv(mod, name=None):
    try:
        m = __import__(mod)
        v = getattr(m, "__version__", "OK")
        print(f"{name or mod}: {v}")
    except Exception as e:
        print(f"{name or mod}: ERR {e}")
pv("aiogram")
pv("pandas")
pv("openpyxl")
pv("xlrd")
try:
    import win32com  # noqa
    print("pywin32: OK")
except Exception as e:
    print("pywin32: ERR", e)
"""
    subprocess.run([str(VENV_PY), "-c", code], check=False)

def write_lock():
    out = subprocess.check_output([str(VENV_PY), "-m", "pip", "freeze"], text=True)
    (HERE / "requirements.lock.txt").write_text(out, encoding="utf-8")
    print("Записан lock-файл: requirements.lock.txt")

if __name__ == "__main__":
    ensure_venv()
    install_deps()
    show_versions()
    write_lock()
    entry = HERE / "main.py"
    print("\nГотово. Запуск бота:")
    print(str(VENV_PY), str(entry))
