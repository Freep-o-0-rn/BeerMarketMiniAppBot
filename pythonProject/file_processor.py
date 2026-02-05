# -*- coding: utf-8 -*-
import os
import re
import time
import zipfile
import logging
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
import numpy as np
import pandas as pd

# ------------------ Общие утилиты ------------------

def _detect_engine(path: str) -> str:
    return "xlrd" if path.lower().endswith(".xls") else "openpyxl"

def _read_headerless(path: str) -> pd.DataFrame:
    return _read_excel_safe(path, header=None)

def _to_float(v: Any) -> float:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return float("nan")
    s = str(v).replace("\xa0", " ").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return float("nan")

def _is_temp_excel_name(path: str) -> bool:
    bn = os.path.basename(path).lower()
    return bn.startswith("~$") or bn.endswith(".tmp") or bn.endswith(".part")

def _fix_missing_sharedstrings_via_zip(src_path: str):
    """
    Если в .xlsx нет xl/sharedStrings.xml — создаём копию архива со
    сгенерированным sharedStrings нужной длины (по max индексу t="s"),
    чтобы openpyxl не падал. Строки будут пустые/заглушки.
    """
    try:
        if not zipfile.is_zipfile(src_path):
            return None

        with zipfile.ZipFile(src_path, "r") as zin:
            names = set(zin.namelist())
            files = {n: zin.read(n) for n in names}

        # Если sharedStrings уже есть — патч не нужен
        if "xl/sharedStrings.xml" in names:
            return None

        # Найдём максимальный индекс shared-string по всем листам
        max_idx = -1
        ws_names = [n for n in names if n.startswith("xl/worksheets/") and n.endswith(".xml")]
        rx = re.compile(rb'<c[^>]*\bt="s"[^>]*>.*?<v>(\d+)</v>', re.DOTALL)
        for wn in ws_names:
            m = rx.findall(files[wn])
            if not m:
                continue
            loc_max = max(int(x) for x in m)
            if loc_max > max_idx:
                max_idx = loc_max

        # Сколько элементов положить в sharedStrings
        count = (max_idx + 1) if max_idx >= 0 else 0

        # Соберём sharedStrings.xml (пустые строки-заглушки)
        if count > 0:
            items = "".join("<si><t></t></si>" for _ in range(count))
        else:
            items = ""  # бывает, что реально нет ссылок t="s"
        sst_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
            f'count="{count}" uniqueCount="{count}">{items}</sst>'
        ).encode("utf-8")

        # Пропишем Override в [Content_Types].xml при отсутствии
        ct_name = "[Content_Types].xml"
        if ct_name in files and b"/xl/sharedStrings.xml" not in files[ct_name]:
            ct = files[ct_name]
            insert = (b'<Override PartName="/xl/sharedStrings.xml" '
                      b'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>')
            if b"</Types>" in ct:
                ct = ct.replace(b"</Types>", insert + b"</Types>")
                files[ct_name] = ct

        out_path = str(Path(src_path).with_suffix(".patched.xlsx"))
        with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
            for n, data in files.items():
                zout.writestr(n, data)
            zout.writestr("xl/sharedStrings.xml", sst_xml)

        logging.info("Patched missing sharedStrings -> %s", out_path)
        return out_path
    except Exception:
        logging.exception("Zip fix failed for %s", src_path)
        return None

def _unblock_motw(path: str) -> None:
    """Снимаем Mark-of-the-Web (Zone.Identifier), чтобы Excel не уходил в Protected View."""
    try:
        import ctypes, ctypes.wintypes as wt
        DeleteFileW = ctypes.windll.kernel32.DeleteFileW
        DeleteFileW.argtypes = [wt.LPCWSTR]
        DeleteFileW.restype = wt.BOOL
        # попытка удалить поток с меткой
        DeleteFileW(f"{os.path.abspath(path)}:Zone.Identifier")
    except Exception:
        pass


def _xlsx_is_valid_zip(path: str) -> bool:
    """Проверяем, что .xlsx — валидный ZIP с базовыми частями Excel."""
    try:
        if not zipfile.is_zipfile(path):
            return False
        with zipfile.ZipFile(path, "r") as z:
            names = set(z.namelist())
        # обязательное: workbook + хотя бы один лист
        if "xl/workbook.xml" not in names:
            return False
        if not any(n.startswith("xl/worksheets/") and n.endswith(".xml") for n in names):
            return False
        # sharedStrings может отсутствовать (inlineStr), это ок — не проверяем
        return True
    except Exception:
        return False

def _wait_file_stable(path: str, attempts: int = 5, delay: float = 0.3) -> bool:
    """Ждём, пока размер файла перестанет меняться (на случай, когда его ещё пишут)."""
    try:
        last = -1
        for _ in range(attempts):
            cur = os.path.getsize(path)
            if cur == last and cur > 0:
                return True
            last = cur
            time.sleep(delay)
        return os.path.getsize(path) > 0
    except Exception:
        return False

#«ожидания стабильности»
def _is_valid_excel_file(path: str) -> bool:
    try:
        if not os.path.isfile(path):
            return False
        if _is_temp_excel_name(path):
            return False
        if not _wait_file_stable(path):
            return False
        # .xlsx дополнительно проверяем как ZIP
        if path.lower().endswith(".xlsx") and not _xlsx_is_valid_zip(path):
            return False
        # отсечём совсем мелкие
        if os.path.getsize(path) < 1024:
            return False
        return True
    except Exception:
        return False

def _read_excel_safe(path: str, header=None):
    _ox_kwargs = dict(engine="openpyxl", engine_kwargs={"data_only": True, "read_only": True})
    ext = os.path.splitext(path)[1].lower()

    if ext == ".xlsx":
        try:
            return pd.read_excel(path, header=header, **_ox_kwargs)
        except Exception as e1:
            msg = str(e1)

            # 1) сначала пробуем zip-патч
            fixed = None
            if "sharedStrings.xml" in msg:
                fixed = _fix_missing_sharedstrings_via_zip(path)
                if fixed:
                    try:
                        logging.info("Retry patched -> openpyxl: %s", os.path.basename(fixed))
                        return pd.read_excel(fixed, header=header, **_ox_kwargs)
                    except Exception:
                        logging.exception("patched read failed for %s", fixed)

            # 2) затем COM-ремонт (после снятия Protected View)
            try:
                repaired = _repair_xlsx_via_excel(path if not fixed else fixed)
                logging.info("Excel repaired -> %s", os.path.basename(repaired))
                return pd.read_excel(repaired, header=header, **_ox_kwargs)
            except Exception:
                logging.exception("Excel COM repair failed for %s", path)

            raise

            # 3) Если COM недоступен — последний шанс: патч sharedStrings + повтор
            if "sharedStrings.xml" in msg:
                fixed = _fix_missing_sharedstrings_via_zip(path)
                if fixed:
                    try:
                        logging.info("Retry patched -> openpyxl: %s", os.path.basename(fixed))
                        return pd.read_excel(fixed, header=header, **_ox_kwargs)
                    except Exception:
                        logging.exception("patched read failed for %s", fixed)

            # 4) сдаёмся
            raise

    if ext == ".xls":
        return pd.read_excel(path, header=header, engine="xlrd")

    return pd.read_excel(path, header=header)


# ---- хелпер: определяем тип книги по «магическим» байтам ----
def _detect_excel_kind(path: str) -> str:
    # 'xlsx' = zip PK\x03\x04, 'xls' = OLE D0 CF 11 E0 A1 B1 1A E1
    try:
        with open(path, "rb") as f:
            sig = f.read(8)
        if sig.startswith(b"PK\x03\x04"):
            return "xlsx"
        if sig.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
            return "xls"
    except Exception:
        logging.exception("detect kind failed for %s", path)
    # если не поняли — смотрим по расширению
    ext = os.path.splitext(path)[1].lower()
    return "xlsx" if ext == ".xlsx" else "xls" if ext == ".xls" else "unknown"


# ------------------ Безопасный враппер для чтения Excel ------------------
def _repair_xlsx_via_excel(src_path: str) -> str:
    """
    Открывает файл в Excel и пересохраняет в .repaired.xlsx.
    Используем late-binding (dynamic.Dispatch) без gencache/makepy.
    Перед открытием снимаем Mark-of-the-Web и пытаемся открыть с CorruptLoad=1.
    """
    from pathlib import Path
    abspath = os.path.abspath(src_path)
    out_path = str(Path(abspath).with_suffix(".repaired.xlsx"))

    _unblock_motw(abspath)  # убрать Protected View, если есть

    try:
        # Late binding, чтобы не упираться в gen_py
        from win32com.client.dynamic import Dispatch
    except Exception as e:
        raise RuntimeError("pywin32 не установлен или повреждён") from e

    excel = None
    try:
        excel = Dispatch("Excel.Application")
        excel.DisplayAlerts = False
        excel.Visible = False

        # Попытка 1: CorruptLoad=1 (Recovery)
        try:
            wb = excel.Workbooks.Open(Filename=abspath, ReadOnly=False, CorruptLoad=1,
                                      IgnoreReadOnlyRecommended=True, Notify=False)
        except Exception:
            # Попытка 2: без CorruptLoad — вдруг сработает
            wb = excel.Workbooks.Open(Filename=abspath, ReadOnly=False,
                                      IgnoreReadOnlyRecommended=True, Notify=False)

        wb.SaveAs(Filename=out_path, FileFormat=51)  # 51 = .xlsx
        wb.Close(SaveChanges=False)
    finally:
        if excel is not None:
            try:
                excel.Quit()
            except Exception:
                pass

    return out_path




# ------------------ Дебиторка (как было) ------------------

def _find_detail_header_row(df_raw: pd.DataFrame) -> Optional[int]:
    """
    Ищем начало детальной таблицы: строка с 'Клиент'+'Объект'+'Долг клиента',
    а следующая строка содержит 'Всего'/'Просрочено'/'Дней'.
    """
    n = len(df_raw)
    for i in range(n - 1):
        row = " ".join(str(x).strip().lower() for x in df_raw.iloc[i].tolist() if str(x) != "nan")
        row2 = " ".join(str(x).strip().lower() for x in df_raw.iloc[i+1].tolist() if str(x) != "nan")
        if ("клиент" in row and "объект" in row and "долг клиента" in row) and any(k in row2 for k in ["всего","просроч","дней"]):
            return i
    return None

def _flatten_columns(cols: pd.MultiIndex) -> List[str]:
    flat: List[str] = []
    for tpl in cols:
        parts = [str(x).strip().lower() for x in (tpl if isinstance(tpl, tuple) else (tpl,))]
        parts = [p for p in parts if p and p != "nan"]
        name = " ".join(parts)
        name = name.replace("состояние взаиморасчетов ", "")
        name = re.sub(r"\s+", " ", name).strip()
        flat.append(name)
    return flat

def _extract_report_date(df_raw: pd.DataFrame) -> Optional[str]:
    for i in range(min(25, len(df_raw))):
        row = " ".join(str(x) for x in df_raw.iloc[i].tolist() if str(x) != "nan")
        m = re.search(r"Дата\s*отч[её]та[:\s]+(\d{2}\.\d{2}\.\d{4})", row)
        if m:
            return m.group(1)
    return None

def read_debt_file(path: str) -> Tuple[pd.DataFrame, Optional[str]]:
    df_raw = _read_headerless(path)
    report_date = _extract_report_date(df_raw)
    start = _find_detail_header_row(df_raw)
    if start is None:
        raise ValueError("Не удалось найти шапку детальной таблицы (Клиент/Объект/Долг клиента).")

    engine = _detect_engine(path)
    df = pd.read_excel(path, header=[start, start+1], engine=engine)

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = _flatten_columns(df.columns)
    else:
        df.columns = [str(c).strip().lower() for c in df.columns]

    df = df.dropna(axis=1, how="all")
    logging.info("Нашли заголовки детальной таблицы на строке: %d", start + 1)
    return df, report_date

CLIENT_MARKERS = re.compile(r"\b(ооо|ип|зао|оао|ао|тоо)\b", re.IGNORECASE)
REALIZATION_MARKERS = re.compile(r"(реализац|накладн|сч[её]т|отгруз)", re.IGNORECASE)

DOC_PATTERNS = [
    r"\bбе\d{2,}-\d+\b",
    r"\bbe\d{2,}-\d+\b",
    r"\b[а-яa-z]{1,5}\d{2,}-\d+\b",
    r"№\s*[\w\-]+",
]

def _extract_address_from_name(name: str) -> str:
    if not name:
        return ""
    m = re.search(r"\(([^)]+)\)", name)
    return m.group(1).strip() if m else ""

def _extract_doc_numbers(text: str) -> List[str]:
    if not text:
        return []
    out: List[str] = []
    for p in DOC_PATTERNS:
        out += re.findall(p, text, flags=re.IGNORECASE)
    out = [re.sub(r"\s+", " ", x).strip() for x in out]
    seen = set(); uniq = []
    for x in out:
        k = x.lower()
        if k in seen: continue
        seen.add(k); uniq.append(x)
    return uniq

def _extract_doc_date(text: str) -> Optional[str]:
    m = re.search(r"от\s+(\d{2}\.\d{2}\.\d{4})", text)
    return m.group(1) if m else None

def _get_col_all(df: pd.DataFrame, must: List[str], exclude: Optional[List[str]] = None) -> Optional[str]:
    exclude = exclude or []
    for c in df.columns:
        name = c.lower()
        if all(m in name for m in must) and not any(e in name for e in exclude):
            return c
    return None

def parse_clients(df: pd.DataFrame) -> List[Dict[str, Any]]:
    col_text = _get_col_all(df, ["объект", "расчет"]) or _get_col_all(df, ["клиент"], exclude=["долг"])
    col_total    = _get_col_all(df, ["долг", "клиента", "всего"])
    col_overd    = _get_col_all(df, ["долг", "клиента", "просроч"])
    col_days     = _get_col_all(df, ["дней"])
    col_our_debt = _get_col_all(df, ["наш", "долг"])

    if not col_text or not col_total:
        logging.error("Критичные колонки не найдены (text/total).")
        return []

    for c in [col_total, col_overd, col_days, col_our_debt]:
        if c and c in df.columns:
            if c == col_days:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            else:
                df[c] = df[c].apply(_to_float)

    results: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    our_debt_sum_rows: float = 0.0
    our_debt_hdr: float = float("nan")

    def flush():
        nonlocal current, our_debt_sum_rows, our_debt_hdr
        if not current:
            return

        our_debt_final = our_debt_hdr if np.isfinite(our_debt_hdr) else our_debt_sum_rows
        if np.isfinite(our_debt_hdr) and not np.isclose(our_debt_hdr, our_debt_sum_rows, atol=0.01):
            logging.warning("Несовпадение 'наш долг' у клиента '%s': шапка=%.2f, по строкам=%.2f",
                            current["client"], our_debt_hdr, our_debt_sum_rows)

        current["our_debt"] = float(our_debt_final) if np.isfinite(our_debt_final) else 0.0
        current["our_debt_hdr"] = float(our_debt_hdr) if np.isfinite(our_debt_hdr) else None
        current["our_debt_sum_rows"] = float(our_debt_sum_rows)

        current["realizations_count"] = int(current.get("realizations_count", 0))
        current["total_amount"] = float(current.get("total_amount", 0.0) or 0.0)
        current["overdue_amount"] = float(current.get("overdue_amount", 0.0) or 0.0)
        current["max_days"] = int(current.get("max_days", 0) or 0)

        if current.get("realization_numbers"):
            seen=set(); uniq=[]
            for x in current["realization_numbers"]:
                k=x.lower()
                if k in seen: continue
                seen.add(k); uniq.append(x)
            current["realization_numbers"]=uniq

        results.append(current)
        current = None
        our_debt_sum_rows = 0.0
        our_debt_hdr = float("nan")

    for _, row in df.iterrows():
        txt = str(row.get(col_text, "") or "").strip()
        if not txt or txt.lower().startswith("итог"):
            continue

        is_real = bool(REALIZATION_MARKERS.search(txt))
        is_client = (not is_real) and bool(CLIENT_MARKERS.search(txt))

        if is_client:
            flush()

            total = _to_float(row.get(col_total, float("nan")))
            overdue = _to_float(row.get(col_overd, total))
            days_val = row.get(col_days, np.nan)
            max_days = int(days_val) if not pd.isna(days_val) else 0
            our_debt_hdr = _to_float(row.get(col_our_debt, float("nan"))) if col_our_debt else float("nan")

            current = {
                "client": txt,
                "address": _extract_address_from_name(txt),
                "realizations_count": 0,
                "realization_numbers": [],
                "docs": [],
                "total_amount": round(total if np.isfinite(total) else 0.0, 2),
                "overdue_amount": round(overdue if np.isfinite(overdue) else 0.0, 2),
                "max_days": max_days,
            }
            continue

        if is_real and current is not None:
            current["realizations_count"] += 1
            nums = _extract_doc_numbers(txt)
            if nums:
                current["realization_numbers"].extend(nums)

            doc_amount = _to_float(row.get(col_overd, row.get(col_total, float("nan"))))
            doc_days = int(row[col_days]) if (col_days and not pd.isna(row.get(col_days, np.nan))) else None
            doc_date = _extract_doc_date(txt)
            doc_our_debt = _to_float(row.get(col_our_debt, float("nan"))) if col_our_debt else float("nan")
            if np.isfinite(doc_our_debt):
                our_debt_sum_rows += doc_our_debt

            current["max_days"] = max(current["max_days"], doc_days or 0)

            current["docs"].append({
                "text": txt,
                "doc_numbers": nums,
                "doc_date": doc_date,
                "amount": float(doc_amount) if np.isfinite(doc_amount) else None,
                "days": doc_days,
                "our_debt_doc": float(doc_our_debt) if np.isfinite(doc_our_debt) else None,
            })

    flush()
    results.sort(key=lambda x: x["total_amount"], reverse=True)
    logging.info("Собрано клиентов: %d", len(results))
    return results

# ------------------ Тара: Ведомость по возвратной таре (клиент) ------------------

# маркеры строк-документов, которые нужно игнорировать (движения)
TARA_DOC_MARKERS = re.compile(
    r"(реализаци|возврат|перемещени|поступлени|списани|оказани[ея]\s+услуг|акт|накладн)",
    re.I
)
# --- Simple cache for parsed TARA files ---
_TARA_CACHE = {}  # path -> (mtime, size, data)
#кэш разбора тары (без повторного парсинга)
def process_tara_cached(path: str):
    try:
        st = os.stat(path)
        sig = (st.st_mtime, st.st_size)
    except FileNotFoundError:
        return process_tara_file(path)  # как было, на всякий

    cached = _TARA_CACHE.get(path)
    if cached and cached[0] == sig[0] and cached[1] == sig[1]:
        return cached[2]

    data = process_tara_file(path)
    _TARA_CACHE[path] = (sig[0], sig[1], data)
    return data

def _to_float_ru(x: Any) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)) or str(x).strip() == "":
        return 0.0
    s = str(x).strip().replace(" ", "").replace("\xa0", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0

def _tara_find_cols(df: pd.DataFrame) -> Tuple[str, str, Optional[str], str]:
    """Находим имена колонок: Клиент, Номенклатура, Регистратор?, Конечный остаток."""
    def _find(colnames, *needles):
        low = {c: str(c).lower() for c in colnames}
        for c, lc in low.items():
            if all(n in lc for n in needles):
                return c
        return None

    col_client  = _find(df.columns, "клиент")
    col_item    = _find(df.columns, "номенклат")
    col_reg     = _find(df.columns, "регистр")
    col_kon_end = _find(df.columns, "конечн", "остат")
    if not all([col_client, col_item, col_kon_end]):
        raise ValueError("Не найдены обязательные колонки (Клиент/Номенклатура/Конечный остаток)")
    return col_client, col_item, col_reg, col_kon_end

def read_tara_file(path: str) -> Tuple[pd.DataFrame, Optional[str]]:
    df_raw = _read_excel_safe(path, header=None)

    # ищем строку шапки (строка содержит 'Клиент' и 'остат')
    header_row = None
    for i, row in enumerate(df_raw.values.tolist()):
        joined = " ".join(str(x) for x in row if str(x).strip() not in ("", "nan")).lower()
        if "клиент" in joined and "остат" in joined:
            header_row = i
            break
    if header_row is None:
        raise ValueError("Не удалось найти заголовок таблицы (строка с 'Клиент').")

    df = _read_excel_safe(path, header=header_row)

    # дата отчёта (первые 6 строк исходника)
    report_date = None
    try:
        top = _read_excel_safe(path, header=None).astype(str).fillna("")
        for i in range(min(6, len(top))):
            line = " ".join(top.iloc[i].tolist())
            m = re.search(r"Период:\s*(\d{2}\.\d{2}\.\d{4}).*?(\d{2}\.\d{2}\.\d{4})", line)
            if m:
                report_date = m.group(2)
                break
    except Exception:
        pass

    return df.dropna(how="all"), report_date



def parse_tara(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Парсинг строк отчёта по возвратной таре"""
    col_names = list(df.columns)
    # Определяем, какие колонки содержат числа
    col_qty_end = None
    for c in col_names:
        if "конеч" in str(c).lower() and "остат" in str(c).lower():
            col_qty_end = c
            break
    if not col_qty_end:
        raise ValueError("Не найдена колонка 'Количество Конечный остаток'")

    results: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    for _, row in df.iterrows():
        text = str(row.iloc[0] or "").strip()
        if not text:
            continue

        # количество по последней колонке
        q_end = _to_float(row.get(col_qty_end, 0))

        # клиент
        if CLIENT_MARKERS.search(text):
            # сохранить предыдущего
            if current:
                current["items"] = [(n, q) for n, q in current["items"] if abs(q) > 1e-9]
                if current["total"] > 0 or current["items"]:
                    results.append(current)
            current = {"client": text, "items": [], "total": q_end}
            continue

        # служебная строка (реализация, акт и т.п.)
        if TARA_DOC_MARKERS.search(text):
            continue

        # номенклатура
        if current and abs(q_end) > 0:
            current["items"].append((text, q_end))

    # финальный клиент
    if current:
        current["items"] = [(n, q) for n, q in current["items"] if abs(q) > 1e-9]
        if current["total"] > 0 or current["items"]:
            results.append(current)

    results.sort(key=lambda x: x.get("total", 0.0), reverse=True)
    logging.info("Тара: собрано клиентов: %d", len(results))
    return results

def process_tara_file(path: str) -> Dict[str, Any]:
    df, report_date = read_tara_file(path)
    items = parse_tara(df)
    return {"report_date": report_date, "items": items}

# ------------------ Автоопределение типа отчёта ------------------

def _guess_is_tara(path: str) -> bool:
    """Грубое определение 'Тара' по заголовку/колонкам (без падений)."""
    try:
        top = _read_excel_safe(path, header=None)
        head_text = " ".join(" ".join(map(str, r)) for r in top.head(4).values.tolist()).lower()
        if "возвратной таре" in head_text or "отчет по возвратной таре" in head_text:
            return True
    except Exception:
        pass
    try:
        df = _read_excel_safe(path, header=6)
        names = " ".join(str(c).lower() for c in df.columns)
        if ("клиент" in names and "номенклат" in names) and ("конечн" in names and "остат" in names):
            return True
    except Exception:
        pass
    name = os.path.basename(path).lower()
    return ("тара" in name) or ("возвратн" in name)

# ------------------ Внешние функции ------------------

def process_file(path: str) -> Dict[str, Any]:
    """
    Универсальная точка: сама определяет тип отчёта и парсит.
    Возвращает {report_date, items}.
    Для дебиторки: items = список клиентов с полями (как раньше).
    Для тары:      items = [{client, total, items:[(name, qty), ...]}, ...]
    """
    if _guess_is_tara(path):
        return process_tara_file(path)
    # иначе — дебиторка
    df, report_date = read_debt_file(path)
    items = parse_clients(df)
    return {"report_date": report_date, "items": items}

def find_latest_download(download_dir: str = "downloads", report_type: str = "debt") -> Optional[str]:
    """Совместимость: вернуть один лучший файл указанного типа."""
    lst = find_latest_downloads(download_dir, report_type, max_count=1)
    return lst[0] if lst else None


def find_latest_downloads(download_dir: str = "downloads",
                          report_type: str = "debt",
                          max_count: int = 5) -> List[str]:
    """
    Возвращаем несколько подходящих файлов (свежие → старые), без lock-файлов '~$...'.
    report_type ∈ {'debt','tara'}
    """
    if not os.path.isdir(download_dir):
        return []

    name_keys = ("тара", "таре", "тары", "возвратн") if report_type == "tara" else ("дебитор", "дз")

    # 1) кандидаты по имени
    cands: List[str] = []
    for fn in os.listdir(download_dir):
        path = os.path.join(download_dir, fn)
        low = fn.lower()
        if low.endswith((".xls", ".xlsx")) and any(k in low for k in name_keys) and _is_valid_excel_file(path):
            cands.append(path)

    # 2) если по имени пусто — fallback по содержимому
    if not cands:
        all_xls = [
            os.path.join(download_dir, fn)
            for fn in os.listdir(download_dir)
            if fn.lower().endswith((".xls", ".xlsx"))
        ]
        all_xls = [p for p in all_xls if _is_valid_excel_file(p)]
        all_xls.sort(key=lambda p: os.path.getmtime(p), reverse=True)

        for p in all_xls:
            try:
                is_tara = _guess_is_tara(p)
            except Exception:
                # грубый фоллбек: считаем .xlsx — вероятно тара, .xls — дебиторка
                is_tara = p.lower().endswith(".xlsx")

            if report_type == "tara" and is_tara:
                cands.append(p)
            elif report_type == "debt" and not is_tara:
                cands.append(p)

    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    # финальный фильтр (ещё раз) — уберём временные/битые/недописанные
    cands = [p for p in cands if _is_valid_excel_file(p)]
    return cands[:max_count]


