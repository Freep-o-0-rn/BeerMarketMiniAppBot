import os
import sqlite3
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path(os.getenv("CLIENTS_DB_PATH", "settings/clients.sqlite3"))

DEFAULT_POSITIONS = [
    "Директор",
    "ЛПР",
    "Управляющий",
    "Администратор",
    "Бухгалтер",
    "Ст. Бармен",
    "Ст. Продавец",
    "Продавец",
]


def _utcnow() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class ClientCardsDB:
    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path or DB_PATH)
        _ensure_parent(self.path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = _dict_factory
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS networks (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS clients (
                    id TEXT PRIMARY KEY,
                    legal_form TEXT NOT NULL CHECK(legal_form IN ('ООО','ИП')),
                    legal_name TEXT NOT NULL,
                    store_name TEXT NOT NULL,
                    address TEXT NOT NULL,
                    overdue_days INTEGER NOT NULL,
                    technician_name TEXT,
                    technician_phone TEXT,
                    technician_id TEXT,
                    sales_rep_user_id INTEGER,
                    sales_rep_name TEXT,
                    owner_user_id INTEGER,
                    network_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(network_id) REFERENCES networks(id) ON DELETE SET NULL,
                    FOREIGN KEY(technician_id) REFERENCES technicians(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS technicians (
                    id TEXT PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    points_csv TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS client_contacts (
                    id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    contact_name TEXT NOT NULL,
                    contact_phone TEXT NOT NULL,
                    contact_position TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS client_user_links (
                    user_id INTEGER NOT NULL,
                    client_id TEXT NOT NULL,
                    can_edit INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, client_id),
                    FOREIGN KEY(client_id) REFERENCES clients(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_clients_sales_rep ON clients(sales_rep_user_id);
                CREATE INDEX IF NOT EXISTS idx_clients_network ON clients(network_id);
                CREATE INDEX IF NOT EXISTS idx_links_user ON client_user_links(user_id);
                """
            )
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(clients)").fetchall()}
            if "technician_id" not in cols:
                conn.execute("ALTER TABLE clients ADD COLUMN technician_id TEXT")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_technician ON clients(technician_id)")

        try:
            os.chmod(self.path, 0o600)
        except Exception:
            pass

    def create_client(self, payload: Dict[str, Any], contacts: List[Dict[str, str]]) -> str:
        cid = str(uuid.uuid4())
        now = _utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO clients(
                    id, legal_form, legal_name, store_name, address, overdue_days,
                    technician_name, technician_phone, sales_rep_user_id, sales_rep_name,
                    technician_id, owner_user_id, network_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cid,
                    payload["legal_form"],
                    payload["legal_name"],
                    payload["store_name"],
                    payload["address"],
                    int(payload["overdue_days"]),
                    payload.get("technician_name"),
                    payload.get("technician_phone"),
                    payload.get("sales_rep_user_id"),
                    payload.get("sales_rep_name"),
                    payload.get("technician_id"),
                    payload.get("owner_user_id"),
                    payload.get("network_id"),
                    now,
                    now,
                ),
            )
            if payload.get("owner_user_id"):
                conn.execute(
                    "INSERT OR REPLACE INTO client_user_links(user_id, client_id, can_edit, created_at) VALUES (?, ?, 1, ?)",
                    (int(payload["owner_user_id"]), cid, now),
                )
            for c in contacts:
                conn.execute(
                    """
                    INSERT INTO client_contacts(id, client_id, contact_name, contact_phone, contact_position, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        cid,
                        c["contact_name"],
                        c["contact_phone"],
                        c["contact_position"],
                        now,
                        now,
                    ),
                )
        return cid

    def find_client(self, legal_form: str, legal_name: str, address: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM clients
                WHERE lower(legal_form) = lower(?)
                  AND lower(trim(legal_name)) = lower(trim(?))
                  AND lower(trim(address)) = lower(trim(?))
                LIMIT 1
                """,
                (legal_form, legal_name, address),
            ).fetchone()

    def add_contact(self, client_id: str, name: str, phone: str, position: str) -> None:
        now = _utcnow()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO client_contacts(id, client_id, contact_name, contact_phone, contact_position, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), client_id, name, phone, position, now, now),
            )

    def list_clients(self, *, sales_rep_user_id: Optional[int] = None, owner_user_id: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            if owner_user_id is not None:
                rows = conn.execute(
                    """
                    SELECT c.*
                    FROM clients c
                    JOIN client_user_links l ON l.client_id = c.id
                    WHERE l.user_id = ?
                    ORDER BY c.updated_at DESC
                    """,
                    (int(owner_user_id),),
                ).fetchall()
            elif sales_rep_user_id is not None:
                rows = conn.execute(
                    "SELECT * FROM clients WHERE sales_rep_user_id = ? ORDER BY updated_at DESC",
                    (int(sales_rep_user_id),),
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM clients ORDER BY updated_at DESC").fetchall()
        return rows

    def get_client(self, client_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
            if not row:
                return None
            contacts = conn.execute(
                "SELECT id, contact_name, contact_phone, contact_position FROM client_contacts WHERE client_id = ? ORDER BY created_at",
                (client_id,),
            ).fetchall()
            network = None
            if row.get("network_id"):
                network = conn.execute("SELECT * FROM networks WHERE id = ?", (row["network_id"],)).fetchone()
            linked_clients = []
            if row.get("network_id"):
                linked_clients = conn.execute(
                    "SELECT id, legal_form, legal_name, store_name FROM clients WHERE network_id = ? ORDER BY legal_name",
                    (row["network_id"],),
                ).fetchall()
            row["contacts"] = contacts
            if row.get("technician_id"):
                row["technician"] = conn.execute(
                    "SELECT id, full_name, phone FROM technicians WHERE id = ?",
                    (row["technician_id"],),
                ).fetchone()
            else:
                row["technician"] = None
            row["network"] = network
            row["network_clients"] = linked_clients
            return row

    def update_client(self, client_id: str, patch: Dict[str, Any]) -> None:
        if not patch:
            return
        allowed = {
            "legal_form", "legal_name", "store_name", "address", "overdue_days",
            "technician_name", "technician_phone", "sales_rep_user_id", "sales_rep_name",
            "technician_id", "owner_user_id", "network_id",
        }
        parts = []
        vals = []
        for k, v in patch.items():
            if k not in allowed:
                continue
            parts.append(f"{k} = ?")
            vals.append(v)
        if not parts:
            return
        parts.append("updated_at = ?")
        vals.append(_utcnow())
        vals.append(client_id)
        with self._connect() as conn:
            conn.execute(f"UPDATE clients SET {', '.join(parts)} WHERE id = ?", tuple(vals))

    def ensure_network(self, name: str) -> str:
        name = (name or "").strip()
        if not name:
            raise ValueError("network name is required")
        with self._connect() as conn:
            row = conn.execute("SELECT id FROM networks WHERE lower(name) = lower(?)", (name,)).fetchone()
            if row:
                return row["id"]
            nid = str(uuid.uuid4())
            now = _utcnow()
            conn.execute(
                "INSERT INTO networks(id, name, note, created_at, updated_at) VALUES (?, ?, '', ?, ?)",
                (nid, name, now, now),
            )
            return nid

    def set_user_link(self, user_id: int, client_id: str, can_edit: bool = True) -> None:
        now = _utcnow()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO client_user_links(user_id, client_id, can_edit, created_at) VALUES (?, ?, ?, ?)",
                (int(user_id), client_id, 1 if can_edit else 0, now),
            )

    def delete_client(self, client_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))


    def user_can_access(self, user_id: int, role: str, client_id: str) -> bool:
        if role == "admin":
            return True
        with self._connect() as conn:
            if role == "sales_rep":
                row = conn.execute("SELECT 1 FROM clients WHERE id = ? AND sales_rep_user_id = ?", (client_id, int(user_id))).fetchone()
                return bool(row)
            row = conn.execute(
                "SELECT can_edit FROM client_user_links WHERE user_id = ? AND client_id = ?",
                (int(user_id), client_id),
            ).fetchone()
            return bool(row)

    def list_technicians(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM technicians ORDER BY full_name COLLATE NOCASE").fetchall()

    def get_technician(self, technician_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            return conn.execute("SELECT * FROM technicians WHERE id = ?", (technician_id,)).fetchone()

    def create_technician(self, full_name: str, phone: str, points_csv: str = "") -> str:
        tid = str(uuid.uuid4())
        now = _utcnow()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO technicians(id, full_name, phone, points_csv, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (tid, full_name.strip(), phone.strip(), points_csv.strip(), now, now),
            )
        return tid

    def update_technician(self, technician_id: str, full_name: str, phone: str, points_csv: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE technicians SET full_name = ?, phone = ?, points_csv = ?, updated_at = ? WHERE id = ?",
                (full_name.strip(), phone.strip(), points_csv.strip(), _utcnow(), technician_id),
            )

    def delete_technician(self, technician_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE clients SET technician_id = NULL WHERE technician_id = ?",
                (technician_id,),
            )
            conn.execute("DELETE FROM technicians WHERE id = ?", (technician_id,))

    def export_masked_summary(self) -> Dict[str, Any]:
        with self._connect() as conn:
            clients = conn.execute("SELECT legal_form, legal_name, store_name, address, overdue_days, sales_rep_name FROM clients").fetchall()
        return {"clients_count": len(clients), "clients": clients}


def format_client_card(card: Dict[str, Any]) -> str:
    network_name = ((card.get("network") or {}).get("name") if isinstance(card.get("network"), dict) else None) or "—"
    contacts = card.get("contacts") or []
    tech = card.get("technician") if isinstance(card.get("technician"), dict) else None
    tech_name = (tech or {}).get("full_name") or card.get("technician_name") or "ТЕСТ"
    tech_phone = (tech or {}).get("phone") or card.get("technician_phone") or "+79999999999"
    lines = [
        f"<b>{card.get('legal_form')} {card.get('legal_name')}</b>",
        f"Магазин: {card.get('store_name') or '—'}",
        f"Адрес: {card.get('address') or '—'}",
        f"Отсрочка: {card.get('overdue_days')} дн.",
        f"Техник: {tech_name} ({tech_phone})",
        f"Торговый представитель: {card.get('sales_rep_name') or '—'}",
        f"Сеть: {network_name}",
        "",
        "<b>Контакты:</b>",
    ]
    if not contacts:
        lines.append("— не добавлены")
    else:
        for i, c in enumerate(contacts, 1):
            lines.append(f"{i}. {c.get('contact_name')} — {c.get('contact_phone')} ({c.get('contact_position')})")
    if card.get("network_clients") and len(card["network_clients"]) > 1:
        lines.append("\n<b>Юрлица в сети:</b>")
        for c in card["network_clients"]:
            lines.append(f"• {c.get('legal_form')} {c.get('legal_name')} ({c.get('store_name')})")
    return "\n".join(lines)
