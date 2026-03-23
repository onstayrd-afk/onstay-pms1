from __future__ import annotations

import base64
import csv
import html
import io
import os
import random
import re
import sqlite3
import traceback
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from PIL import Image
from flask import (
    Flask,
    Response,
    flash,
    redirect,
    render_template_string,
    request,
    send_file,
    send_from_directory,
    session,
    url_for,
)
from icalendar import Calendar
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
# En Vercel (serverless) solo /tmp es escribible: base SQLite y archivos subidos.
if os.environ.get("VERCEL"):
    _tmp = Path("/tmp")
    DB_PATH = Path(os.environ.get("ONSTAY_DB_PATH", str(_tmp / "onstay.db")))
    UPLOAD_DIR = _tmp / "onstay_uploads"
    CONTRACTS_DIR = UPLOAD_DIR / "contracts"
else:
    DB_PATH = BASE_DIR / "onstay.db"
    UPLOAD_DIR = BASE_DIR / "static" / "uploads"
    CONTRACTS_DIR = UPLOAD_DIR / "contracts"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_CONTRACT_EXT = (".pdf",)


def _save_logo_image(file, target_name: str = "logo.png") -> bool:
    if not file or not file.filename:
        return False
    try:
        img = Image.open(file.stream)
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGBA")
        img.save(str(UPLOAD_DIR / target_name), format="PNG")
        return True
    except Exception:
        traceback.print_exc()
        return False


def _save_owner_contract(owner_id: int, file) -> str | None:
    if not file or not file.filename:
        return None
    fn = secure_filename(file.filename)
    if not fn:
        return None
    ext = ""
    for e in ALLOWED_CONTRACT_EXT:
        if fn.lower().endswith(e):
            ext = e
            break
    if not ext:
        return None
    stem = f"owner_{owner_id}_{int(datetime.now().timestamp())}"
    save_name = stem + ext
    path = CONTRACTS_DIR / save_name
    file.save(str(path))
    return save_name


app = Flask(__name__)
app.secret_key = os.environ.get("ONSTAY_SECRET", "onstay-dev-key-change-in-production")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


def migrate_db() -> None:
    with db() as conn:
        tables = [
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        ]
        if "cleaning_tasks" in tables:
            cols = table_columns(conn, "cleaning_tasks")
            if "cleaner_phone" not in cols:
                conn.execute("ALTER TABLE cleaning_tasks ADD COLUMN cleaner_phone TEXT")
            if "scheduled_time" not in cols:
                conn.execute("ALTER TABLE cleaning_tasks ADD COLUMN scheduled_time TEXT")
            if "cleaning_key_note" not in cols:
                conn.execute("ALTER TABLE cleaning_tasks ADD COLUMN cleaning_key_note TEXT")
        if "invoices" in tables:
            cols = table_columns(conn, "invoices")
            if "saved_for_accounting" not in cols:
                conn.execute(
                    "ALTER TABLE invoices ADD COLUMN saved_for_accounting INTEGER NOT NULL DEFAULT 0"
                )
            if "saved_at" not in cols:
                conn.execute("ALTER TABLE invoices ADD COLUMN saved_at TEXT")
            if "saved_by_username" not in cols:
                conn.execute("ALTER TABLE invoices ADD COLUMN saved_by_username TEXT")
        if "owners" in tables:
            ocols = table_columns(conn, "owners")
            if "client_contract_id" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN client_contract_id TEXT")
            if "bank_name" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN bank_name TEXT")
            if "transaction_type" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN transaction_type TEXT")
            if "owner_property_address" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN owner_property_address TEXT")
            if "owner_percentage" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN owner_percentage REAL")
            if "contract_start_date" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN contract_start_date TEXT")
            if "contract_end_date" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN contract_end_date TEXT")
            if "contract_filename" not in ocols:
                conn.execute("ALTER TABLE owners ADD COLUMN contract_filename TEXT")
        if "app_settings" in tables:
            conn.execute(
                "UPDATE app_settings SET value = 'OnstayRd' WHERE key = 'company_name' AND value IN ('ON STAY Property Management', 'ON STAY')"
            )
        if "reservations" in tables:
            rcols = table_columns(conn, "reservations")
            if "checkin_time" not in rcols:
                conn.execute("ALTER TABLE reservations ADD COLUMN checkin_time TEXT")
            if "checkout_time" not in rcols:
                conn.execute("ALTER TABLE reservations ADD COLUMN checkout_time TEXT")
            if "cleaning_in_charge_name" not in rcols:
                conn.execute("ALTER TABLE reservations ADD COLUMN cleaning_in_charge_name TEXT")
        if "properties" in tables:
            pcols = table_columns(conn, "properties")
            if "access_security_info" not in pcols:
                conn.execute("ALTER TABLE properties ADD COLUMN access_security_info TEXT")
        if "maintenance_billing" not in tables:
            conn.execute(
                """
                CREATE TABLE maintenance_billing (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER NOT NULL,
                    month TEXT NOT NULL,
                    description TEXT NOT NULL,
                    amount REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(owner_id) REFERENCES owners(id)
                )
                """
            )


def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT NOT NULL,
                tax_id TEXT,
                email TEXT,
                phone TEXT,
                client_contract_id TEXT,
                bank_name TEXT,
                transaction_type TEXT,
                owner_property_address TEXT,
                owner_percentage REAL
            );

            CREATE TABLE IF NOT EXISTS properties (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                address TEXT,
                owner_id INTEGER NOT NULL,
                commission_pct REAL NOT NULL DEFAULT 20,
                cleaning_fee_default REAL NOT NULL DEFAULT 0,
                airbnb_ical_url TEXT,
                booking_ical_url TEXT,
                access_security_info TEXT,
                FOREIGN KEY(owner_id) REFERENCES owners(id)
            );

            CREATE TABLE IF NOT EXISTS reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_id INTEGER NOT NULL,
                guest_name TEXT NOT NULL,
                source TEXT NOT NULL,
                checkin TEXT NOT NULL,
                checkout TEXT NOT NULL,
                checkin_time TEXT,
                checkout_time TEXT,
                cleaning_in_charge_name TEXT,
                gross_amount REAL NOT NULL,
                bank_cost REAL NOT NULL DEFAULT 0,
                cleaning_fee REAL NOT NULL DEFAULT 0,
                general_cost REAL NOT NULL DEFAULT 0,
                notes TEXT,
                external_uid TEXT,
                UNIQUE(property_id, external_uid),
                FOREIGN KEY(property_id) REFERENCES properties(id)
            );

            CREATE TABLE IF NOT EXISTS cleaning_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reservation_id INTEGER NOT NULL,
                cleaner_name TEXT NOT NULL,
                scheduled_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                notes TEXT,
                cleaning_key_note TEXT,
                FOREIGN KEY(reservation_id) REFERENCES reservations(id)
            );

            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                subtotal REAL NOT NULL,
                commission REAL NOT NULL,
                net_amount REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                FOREIGN KEY(owner_id) REFERENCES owners(id)
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS maintenance_billing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                description TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(owner_id) REFERENCES owners(id)
            );
            """
        )
    migrate_db()
    _seed_defaults()


def _seed_defaults() -> None:
    defaults = [
        ("company_name", "OnstayRd"),
        ("company_email", "onstayrd@gmail.com"),
        ("company_phone", "829-475-5974"),
        ("company_phone_whatsapp", "18294755974"),
        ("legal_note", "Persona fisica - facturacion en USD"),
    ]
    with db() as conn:
        for k, v in defaults:
            conn.execute(
                "INSERT OR IGNORE INTO app_settings(key, value) VALUES (?, ?)",
                (k, v),
            )
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count == 0:
            users = [
                ("admin", "administrador", "ONSTAY2026"),
                ("secretaria", "secretaria", "Secretaria2026"),
                ("contadora", "contadora", "Contadora2026"),
            ]
            for username, role, pwd in users:
                conn.execute(
                    "INSERT INTO users(username, password_hash, role) VALUES (?, ?, ?)",
                    (username, generate_password_hash(pwd), role),
                )


def get_setting(key: str, default: str = "") -> str:
    with db() as conn:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    execute(
        "INSERT INTO app_settings(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def fetch_rows(query: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(query, params).fetchall()


def execute(query: str, params: tuple[Any, ...] = ()) -> None:
    with db() as conn:
        conn.execute(query, params)


def execute_insert(query: str, params: tuple[Any, ...]) -> int:
    with db() as conn:
        cur = conn.execute(query, params)
        return cur.lastrowid


def parse_month(month_str: str) -> tuple[str, str]:
    base = datetime.strptime(month_str, "%Y-%m")
    start = base.strftime("%Y-%m-01")
    if base.month == 12:
        end = f"{base.year + 1}-01-01"
    else:
        end = f"{base.year}-{base.month + 1:02d}-01"
    return start, end


def format_date_time(d: str, t: str | None) -> str:
    d = (d or "").strip()
    tm = (t or "").strip()
    if not d:
        return "—"
    if tm and len(tm) >= 5:
        return f"{d} {tm[:5]}"
    return d


def row_get(row: sqlite3.Row, key: str, default: str = "") -> str:
    try:
        v = row[key]
        if v is None:
            return default
        return str(v)
    except (KeyError, IndexError, TypeError):
        return default


def month_label_es(month_str: str) -> str:
    try:
        base = datetime.strptime(month_str, "%Y-%m")
        meses = (
            "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "octubre",
            "noviembre",
            "diciembre",
        )
        return f"{meses[base.month - 1].capitalize()} {base.year}"
    except Exception:
        return month_str


def login_required(f: Any) -> Any:
    @wraps(f)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if not session.get("user_id"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)

    return wrapped


def roles_allowed(*allowed: str) -> Any:
    def decorator(f: Any) -> Any:
        @wraps(f)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if not session.get("user_id"):
                return redirect(url_for("login", next=request.path))
            role = session.get("role", "")
            if role not in allowed and role != "administrador":
                if role == "contadora" and request.path.startswith("/invoices") and not request.path.startswith("/invoices/archived"):
                    flash(
                        "La contadora solo ve Archivo contable. Entra como admin o secretaria para Facturas y Guardar en archivo."
                    )
                else:
                    flash("No tienes permiso para esta seccion.")
                return redirect(url_for("dashboard"))
            return f(*args, **kwargs)

        return wrapped

    return decorator


# --- Estilos globales (panel) ---
LAYOUT_HEAD = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <base target="_self" />
  <meta name="theme-color" content="#9e7f44" />
  <meta name="apple-mobile-web-app-capable" content="yes" />
  <meta name="mobile-web-app-capable" content="yes" />
  <title>{{ title }} | OnstayRd</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,500;0,600;0,700;1,500&family=Source+Sans+3:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    :root {
      --on-cream: #f2e9de;
      --on-paper: #faf7f2;
      --on-gold: #9e7f44;
      --on-gold-dark: #7f6638;
      --on-ink: #2c2618;
      --on-border: #e6dfcf;
    }
    body { font-family: 'Source Sans 3', system-ui, sans-serif; background: var(--on-paper); color: var(--on-ink); min-height: 100vh; }
    h1, h2, h3, h4, .font-serif { font-family: 'Cormorant Garamond', Georgia, serif; }
    .navbar-onstay {
      background: linear-gradient(180deg, #fff 0%, #faf7f2 100%);
      border-bottom: 1px solid var(--on-border);
      box-shadow: 0 4px 24px rgba(44, 38, 24, 0.06);
    }
    .brand-title { color: var(--on-gold); font-weight: 700; letter-spacing: 0.02em; }
    .nav-link { font-weight: 500; color: var(--on-ink) !important; padding: 0.6rem 0.9rem !important; border-radius: 8px; min-height: 44px; }
    .nav-link:hover { background: var(--on-cream); color: var(--on-gold-dark) !important; }
    .card-onstay { border: 1px solid var(--on-border); border-radius: 12px; background: #fff; box-shadow: 0 2px 12px rgba(44, 38, 24, 0.04); }
    .btn-brand { background: var(--on-gold); border: none; color: #fff; font-weight: 600; padding: 0.5rem 1.25rem; border-radius: 8px; }
    .btn-brand:hover { background: var(--on-gold-dark); color: #fff; }
    .btn-outline-gold { border-color: var(--on-gold); color: var(--on-gold); font-weight: 600; }
    .btn-outline-gold:hover { background: var(--on-cream); color: var(--on-gold-dark); border-color: var(--on-gold-dark); }
    .logo-nav { width: 52px; height: 52px; object-fit: contain; border-radius: 50%; background: #fff; border: 1px solid var(--on-border); }
    .stat-num { font-family: 'Cormorant Garamond', serif; font-size: 1.75rem; font-weight: 700; color: var(--on-gold-dark); }
    .footer-mini { font-size: 0.8rem; color: #6b6356; }
    .badge-role { background: var(--on-cream); color: var(--on-gold-dark); font-weight: 600; }
  </style>
</head>
<body>
  <nav class="navbar navbar-expand-lg navbar-onstay py-3 mb-4">
    <div class="container">
      <a class="navbar-brand d-flex align-items-center gap-2" href="/">
        <img class="logo-nav" src="/logo" alt="OnstayRd" onerror="this.style.display='none'"/>
        <span class="brand-title font-serif fs-4">OnstayRd</span>
      </a>
      <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navMain">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navMain">
        <ul class="navbar-nav ms-auto align-items-lg-center gap-lg-1">
          {{ nav_links|safe }}
        </ul>
        <div class="ms-lg-3 mt-2 mt-lg-0 d-flex align-items-center gap-2">
          {% if session.get('username') %}
            <span class="badge badge-role text-capitalize">{{ session.get('role', '') }}</span>
            <a class="btn btn-sm btn-outline-secondary" href="/logout">Salir</a>
          {% endif %}
        </div>
      </div>
    </div>
  </nav>
  <main class="container pb-5">
    {% with messages = get_flashed_messages() %}
      {% if messages %}
        {% for m in messages %}
          <div class="alert alert-info border-0 shadow-sm">{{ m }}</div>
        {% endfor %}
      {% endif %}
    {% endwith %}
    {{ content|safe }}
  </main>
  <footer class="container pb-4 footer-mini text-center">
    {{ get_setting('company_name') }} &middot;
    <a href="mailto:{{ get_setting('company_email') }}">{{ get_setting('company_email') }}</a> &middot;
    {{ get_setting('company_phone') }}
  </footer>
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""


def nav_html(role: str) -> str:
    items: list[tuple[str, str, tuple[str, ...]]] = [
        ("/", "Inicio", ("administrador", "secretaria", "contadora")),
        ("/owners", "Propietarios", ("administrador", "secretaria")),
        ("/properties", "Propiedades", ("administrador", "secretaria")),
        ("/reservations", "Reservas", ("administrador", "secretaria")),
        ("/cleaning", "Limpieza", ("administrador", "secretaria")),
        ("/security", "Seguridad", ("administrador", "secretaria")),
        ("/settlements", "Liquidaciones", ("administrador", "contadora", "secretaria")),
        ("/maintenance-billing", "Fact. mantenimiento", ("administrador", "contadora", "secretaria")),
        ("/invoices", "Facturas", ("administrador", "secretaria")),
        ("/invoices/archived", "Archivo contable", ("administrador", "contadora", "secretaria")),
        ("/settings", "Empresa", ("administrador",)),
        ("/settings/passwords", "Claves acceso", ("administrador",)),
        ("/test-mensaje", "Prueba WhatsApp", ("administrador",)),
        ("/branding", "Logo", ("administrador",)),
    ]
    links = []
    for href, label, roles in items:
        if role in roles or role == "administrador":
            links.append(f'<li class="nav-item"><a class="nav-link" href="{href}">{label}</a></li>')
    return "\n".join(links)


def layout(title: str, content: str) -> str:
    role = session.get("role", "")
    return render_template_string(
        LAYOUT_HEAD,
        title=title,
        content=content,
        nav_links=nav_html(role),
        get_setting=get_setting,
    )


def normalize_whatsapp_digits(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits.startswith("8"):
        digits = "1" + digits
    if len(digits) == 10 and digits.startswith("9"):
        digits = "1" + digits
    return digits


def whatsapp_url(phone: str, message: str) -> str:
    num = normalize_whatsapp_digits(phone)
    if not num:
        return "#"
    return f"https://wa.me/{num}?text={quote(message, safe='')}"


RD_TZ = timezone(timedelta(hours=-4))


def _hora_actual_rd() -> str:
    return datetime.now(RD_TZ).strftime("%H:%M")


def build_cleaning_whatsapp_message(
    cleaner_name: str,
    property_name: str,
    address: str,
    scheduled_date: str,
    scheduled_time: str,
    checkin_guest: str,
    checkout_guest: str,
    guest_name: str,
    access_security: str,
    cleaning_key_note: str,
    task_notes: str,
    max_len: int = 1800,
) -> str:
    hora_rd = _hora_actual_rd()
    brand = (get_setting("company_name") or "OnstayRd").strip()
    lines = [
        f"Hola {cleaner_name},",
        "",
        f"*{brand} — RECORDATORIO DE LIMPIEZA*",
        "",
        f"*Propiedad:*\n{property_name}",
        f"*Direccion:*\n{address}",
        "",
        f"*Fecha limpieza:* {scheduled_date}",
        f"*Hora programada:* {scheduled_time}",
        f"*Hora actual RD (mensaje):* {hora_rd}",
        "",
        f"*Check-in huesped:* {checkin_guest}",
        f"*Check-out huesped:* {checkout_guest}",
        f"*Huesped:* {guest_name}",
        "",
    ]
    acc = (access_security or "").strip()
    if acc:
        lines.append("*CLAVE / CODIGO / ACCESO AL APTO:*")
        lines.append(acc)
    kn = (cleaning_key_note or "").strip()
    if kn:
        lines.append("")
        lines.append(f"*Llave / llavero / caja:*\n{kn}")
    tn = (task_notes or "").strip()
    if tn:
        lines.append("")
        lines.append(f"*Notas de la tarea:*\n{tn}")
    lines.append("")
    lines.append(
        f"*{brand}* | {get_setting('company_phone')} | {get_setting('company_email')}"
    )
    text = "\n".join(lines)
    if len(text) > max_len:
        text = text[: max_len - 20] + "\n...(mensaje recortado)"
    return text


def _safe_redirect_target(nxt: str) -> str:
    nxt = (nxt or "").strip()
    if not nxt.startswith("/"):
        return url_for("dashboard")
    if nxt.startswith("//") or ".." in nxt:
        return url_for("dashboard")
    return nxt


@app.route("/login", methods=["GET", "POST"])
def login() -> Any:
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    next_param = request.args.get("next") or ""
    if request.method == "POST":
        username = (request.form.get("username") or "").strip().lower()
        password = request.form.get("password") or ""
        next_after = request.form.get("next") or request.args.get("next") or ""
        row = fetch_rows(
            "SELECT id, password_hash, role FROM users WHERE username = ?",
            (username,),
        )
        if row and check_password_hash(row[0]["password_hash"], password):
            session["user_id"] = row[0]["id"]
            session["username"] = username
            session["role"] = row[0]["role"]
            return redirect(_safe_redirect_target(next_after))
        flash("Usuario o contraseña incorrectos.")
        next_param = request.form.get("next") or next_param
    return render_template_string(
        """
        <!doctype html>
        <html lang="es"><head>
        <meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>Inicio de sesi&oacute;n | OnstayRd</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@600;700&family=Source+Sans+3:wght@400;600&display=swap" rel="stylesheet">
        <style>
          body { min-height: 100vh; background: linear-gradient(145deg, #f2e9de 0%, #faf7f2 50%, #fff 100%); font-family: 'Source Sans 3', sans-serif; }
          .card-login { border-radius: 16px; border: 1px solid #e6dfcf; box-shadow: 0 12px 40px rgba(44,38,24,.08); }
          h1 { font-family: 'Cormorant Garamond', serif; color: #9e7f44; }
          .login-logo { width: 72px; height: 72px; object-fit: contain; border-radius: 50%; background: #fff; border: 1px solid #e6dfcf; }
        </style>
        </head><body class="d-flex align-items-center py-5">
        <div class="container" style="max-width: 440px;">
          <div class="text-center mb-3">
            <img class="login-logo mb-2" src="/logo" alt="" onerror="this.style.display='none'"/>
            <h1 class="mb-0">OnstayRd</h1>
            <p class="text-muted small mb-1">Property Management</p>
            <h2 class="h5 text-dark mt-3 mb-0 fw-semibold">Inicio de sesi&oacute;n</h2>
            <p class="small text-muted">Ingresa tu usuario y contrase&ntilde;a para continuar</p>
          </div>
          <div class="card card-login p-4 bg-white">
            {% with messages = get_flashed_messages() %}
              {% if messages %}{% for m in messages %}<div class="alert alert-warning py-2 small">{{ m }}</div>{% endfor %}{% endif %}
            {% endwith %}
            <form method="post" action="{{ url_for('login') }}">
              {% if next_param %}
              <input type="hidden" name="next" value="{{ next_param|e }}">
              {% endif %}
              <label class="form-label fw-semibold">Usuario</label>
              <input class="form-control mb-3" name="username" autocomplete="username" required autofocus placeholder="admin, secretaria o contadora">
              <label class="form-label fw-semibold">Contraseña</label>
              <input class="form-control mb-4" type="password" name="password" autocomplete="current-password" required>
              <button type="submit" class="btn w-100 text-white fw-semibold py-2" style="background:#9e7f44;border-radius:8px;">Entrar al sistema</button>
            </form>
            <p class="small text-muted mt-3 mb-0 text-center">Claves por defecto: admin / secretaria / contadora (cambialas en Claves acceso)</p>
          </div>
        </div>
        </body></html>
        """,
        next_param=next_param,
    )


@app.get("/logout")
def logout() -> Any:
    session.clear()
    flash("Sesión cerrada.")
    return redirect(url_for("login"))


@app.get("/")
@login_required
def dashboard() -> str:
    stats = {
        "properties": fetch_rows("SELECT COUNT(*) AS c FROM properties")[0]["c"],
        "owners": fetch_rows("SELECT COUNT(*) AS c FROM owners")[0]["c"],
        "reservations": fetch_rows("SELECT COUNT(*) AS c FROM reservations")[0]["c"],
        "pending_cleaning": fetch_rows(
            "SELECT COUNT(*) AS c FROM cleaning_tasks WHERE status != 'done'"
        )[0]["c"],
    }
    income = fetch_rows(
        "SELECT IFNULL(SUM(gross_amount), 0) AS total FROM reservations"
    )[0]["total"]
    html = f"""
    <div class="row align-items-center mb-4">
      <div class="col-auto"><img class="logo-nav" style="width:72px;height:72px;" src="/logo" alt=""/></div>
      <div class="col">
        <h1 class="mb-1">Panel administrativo</h1>
        <p class="text-muted mb-0">{get_setting('legal_note')}</p>
      </div>
    </div>
    <div class="row g-3 mb-4">
      <div class="col-md-3"><div class="card-onstay p-4"><div class="text-muted small">Propiedades</div><div class="stat-num">{stats["properties"]}</div></div></div>
      <div class="col-md-3"><div class="card-onstay p-4"><div class="text-muted small">Propietarios</div><div class="stat-num">{stats["owners"]}</div></div></div>
      <div class="col-md-3"><div class="card-onstay p-4"><div class="text-muted small">Reservas</div><div class="stat-num">{stats["reservations"]}</div></div></div>
      <div class="col-md-3"><div class="card-onstay p-4"><div class="text-muted small">Limpiezas pendientes</div><div class="stat-num">{stats["pending_cleaning"]}</div></div></div>
    </div>
    <div class="card-onstay p-4">
      <h3 class="font-serif mb-2">Ingresos acumulados</h3>
      <p class="display-6 fw-bold text-success mb-1">${income:,.2f} <small class="fs-6 text-muted">USD</small></p>
      <p class="mb-0 text-muted">Contacto: {get_setting('company_email')} &middot; {get_setting('company_phone')}</p>
    </div>
    """
    return layout("Inicio", html)


@app.get("/logo")
def logo() -> Any:
    for candidate in ("logo.png", "logo.jpg", "logo.jpeg"):
        logo_path = UPLOAD_DIR / candidate
        if logo_path.exists():
            return send_from_directory(UPLOAD_DIR, candidate)
    return Response(status=204)


@app.get("/invoice-logo")
def invoice_logo() -> Any:
    for candidate in ("invoice_logo.png", "invoice_logo.jpg", "invoice_logo.jpeg"):
        logo_path = UPLOAD_DIR / candidate
        if logo_path.exists():
            return send_from_directory(UPLOAD_DIR, candidate)
    return redirect(url_for("logo"))


@app.route("/settings", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador")
def settings_page() -> str:
    if request.method == "POST":
        set_setting("company_name", request.form.get("company_name", ""))
        set_setting("company_email", request.form.get("company_email", ""))
        set_setting("company_phone", request.form.get("company_phone", ""))
        set_setting("company_phone_whatsapp", request.form.get("company_phone_whatsapp", ""))
        set_setting("legal_note", request.form.get("legal_note", ""))
        flash("Datos de empresa actualizados.")
        return redirect(url_for("settings_page"))
    return layout(
        "Empresa",
        f"""
        <div class="card-onstay p-4 mx-auto" style="max-width:640px;">
          <p class="mb-3"><a class="btn btn-sm btn-outline-secondary" href="/settings/passwords">Claves de acceso (secretaria / contadora)</a></p>
          <h2 class="font-serif mb-3">Datos para documentos y pie de pagina</h2>
          <form method="post">
            <label class="form-label">Nombre comercial</label>
            <input class="form-control mb-2" name="company_name" value="{get_setting('company_name')}">
            <label class="form-label">Correo</label>
            <input class="form-control mb-2" name="company_email" value="{get_setting('company_email')}">
            <label class="form-label">Telefono (mostrar en documentos)</label>
            <input class="form-control mb-2" name="company_phone" value="{get_setting('company_phone')}">
            <label class="form-label">WhatsApp (solo numeros, ej. 18294755974)</label>
            <input class="form-control mb-2" name="company_phone_whatsapp" value="{get_setting('company_phone_whatsapp', '18294755974')}"
                   placeholder="Codigo pais + numero sin espacios">
            <label class="form-label">Nota legal (pie de factura)</label>
            <textarea class="form-control mb-3" name="legal_note" rows="2">{get_setting('legal_note')}</textarea>
            <button class="btn btn-brand" type="submit">Guardar</button>
          </form>
        </div>
        """,
    )


@app.route("/settings/passwords", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador")
def settings_passwords() -> Any:
    if request.method == "POST":
        which = request.form.get("which", "")
        if which == "secretaria":
            p1 = request.form.get("password_sec", "")
            p2 = request.form.get("password_sec2", "")
            if len(p1) < 6:
                flash("La clave de secretaria debe tener al menos 6 caracteres.")
            elif p1 != p2:
                flash("Las claves de secretaria no coinciden.")
            else:
                h = generate_password_hash(p1)
                execute(
                    "UPDATE users SET password_hash = ? WHERE username = ?",
                    (h, "secretaria"),
                )
                flash("Clave de secretaria actualizada.")
            return redirect(url_for("settings_passwords"))
        if which == "contadora":
            p1 = request.form.get("password_cont", "")
            p2 = request.form.get("password_cont2", "")
            if len(p1) < 6:
                flash("La clave de contadora debe tener al menos 6 caracteres.")
            elif p1 != p2:
                flash("Las claves de contadora no coinciden.")
            else:
                h = generate_password_hash(p1)
                execute(
                    "UPDATE users SET password_hash = ? WHERE username = ?",
                    (h, "contadora"),
                )
                flash("Clave de contadora actualizada.")
            return redirect(url_for("settings_passwords"))
        if which == "admin":
            cur = request.form.get("current_admin", "")
            p1 = request.form.get("password_admin", "")
            p2 = request.form.get("password_admin2", "")
            row = fetch_rows(
                "SELECT password_hash FROM users WHERE username = ?",
                ("admin",),
            )
            if not row or not check_password_hash(row[0]["password_hash"], cur):
                flash("Contrasena actual de administrador incorrecta.")
            elif len(p1) < 8:
                flash("La nueva clave de administrador debe tener al menos 8 caracteres.")
            elif p1 != p2:
                flash("Las nuevas claves de administrador no coinciden.")
            else:
                execute(
                    "UPDATE users SET password_hash = ? WHERE username = ?",
                    (generate_password_hash(p1), "admin"),
                )
                flash("Clave de administrador actualizada.")
            return redirect(url_for("settings_passwords"))
        flash("Solicitud no reconocida.")
        return redirect(url_for("settings_passwords"))
    return layout(
        "Claves de acceso",
        """
        <div class="row g-4 justify-content-center">
          <div class="col-lg-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-2">Secretaria</h3>
              <p class="small text-muted">Usuario fijo: <strong>secretaria</strong></p>
              <form method="post">
                <input type="hidden" name="which" value="secretaria">
                <label class="form-label">Nueva contrasena</label>
                <input class="form-control mb-2" type="password" name="password_sec" minlength="6" autocomplete="new-password" required>
                <label class="form-label">Confirmar</label>
                <input class="form-control mb-3" type="password" name="password_sec2" minlength="6" autocomplete="new-password" required>
                <button class="btn btn-brand" type="submit">Guardar clave secretaria</button>
              </form>
            </div>
          </div>
          <div class="col-lg-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-2">Contadora</h3>
              <p class="small text-muted">Usuario fijo: <strong>contadora</strong></p>
              <form method="post">
                <input type="hidden" name="which" value="contadora">
                <label class="form-label">Nueva contrasena</label>
                <input class="form-control mb-2" type="password" name="password_cont" minlength="6" autocomplete="new-password" required>
                <label class="form-label">Confirmar</label>
                <input class="form-control mb-3" type="password" name="password_cont2" minlength="6" autocomplete="new-password" required>
                <button class="btn btn-brand" type="submit">Guardar clave contadora</button>
              </form>
            </div>
          </div>
          <div class="col-lg-10">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-2">Administrador</h3>
              <p class="small text-muted">Usuario fijo: <strong>admin</strong></p>
              <form method="post">
                <input type="hidden" name="which" value="admin">
                <label class="form-label">Contrasena actual</label>
                <input class="form-control mb-2" type="password" name="current_admin" autocomplete="current-password" required>
                <label class="form-label">Nueva contrasena</label>
                <input class="form-control mb-2" type="password" name="password_admin" minlength="8" autocomplete="new-password" required>
                <label class="form-label">Confirmar nueva</label>
                <input class="form-control mb-3" type="password" name="password_admin2" minlength="8" autocomplete="new-password" required>
                <button class="btn btn-outline-gold" type="submit">Cambiar clave administrador</button>
              </form>
            </div>
          </div>
        </div>
        <p class="text-center mt-4"><a href="/settings">Volver a Empresa</a></p>
        """,
    )


def _random_test_message() -> str:
    props = ["Apto 101", "Casa Playa", "Suite Centro", "Estudio Vista Mar"]
    calles = ["Av. Principal 123", "Calle Secundaria 45", "Blvd. Norte 78"]
    codigos = ["1234#", "5678*", "9999#", "0000"]
    msg = (
        "PRUEBA OnstayRd - Mensaje automatico de prueba\n"
        f"Propiedad: {random.choice(props)}\n"
        f"Direccion: {random.choice(calles)}\n"
        f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"Clave prueba: {random.choice(codigos)}\n"
        "Si recibiste esto, el sistema funciona. OnstayRd PMS."
    )
    return msg


@app.route("/test-mensaje", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador")
def test_mensaje() -> Any:
    if request.method == "POST" or request.args.get("phone"):
        phone = (request.form.get("phone") or request.args.get("phone") or "").strip()
        if not phone:
            flash("Escribe un numero de telefono.")
            return redirect(url_for("test_mensaje"))
        num = normalize_whatsapp_digits(phone)
        if not num:
            flash("Numero no valido. Usa codigo pais + numero (ej. 18294755974).")
            return redirect(url_for("test_mensaje"))
        msg = _random_test_message()
        wa_link = whatsapp_url(phone, msg)
        return redirect(wa_link)
    return layout(
        "Prueba WhatsApp",
        """
        <div class="card-onstay p-4 mx-auto" style="max-width:520px;">
          <h2 class="font-serif mb-2">Mensaje de prueba</h2>
          <p class="text-muted small mb-3">
            Envia un mensaje de prueba con datos random a un numero que escribas.
            Se abrira WhatsApp con el texto listo; solo tienes que dar Enviar.
            Sirve para verificar que el sistema de mensajes funciona.
          </p>
          <form method="post" action="/test-mensaje">
            <label class="form-label fw-semibold">Numero (WhatsApp)</label>
            <input class="form-control mb-2" name="phone" placeholder="Ej. 8294755974 o +18294755974" required>
            <button class="btn btn-success w-100 py-2" type="submit">
              Abrir WhatsApp con mensaje de prueba
            </button>
          </form>
          <p class="small text-muted mt-3 mb-0">
            El mensaje incluira datos ficticios (propiedad, direccion, fecha, codigo).
          </p>
        </div>
        """,
    )


@app.route("/branding", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador")
def branding() -> str:
    if request.method == "POST":
        action = request.form.get("action", "main_logo")
        if action == "invoice_logo":
            file = request.files.get("invoice_logo")
            if _save_logo_image(file, "invoice_logo.png"):
                flash("Logo de factura actualizado.")
            else:
                flash("No se pudo procesar el logo de factura. Sube una imagen PNG/JPG valida.")
        else:
            file = request.files.get("logo")
            if _save_logo_image(file, "logo.png"):
                flash("Logo principal actualizado.")
            else:
                flash("No se pudo procesar el logo principal. Sube una imagen PNG/JPG valida.")
        return redirect(url_for("branding"))
    return layout(
        "Logo",
        """
        <div class="card-onstay p-4 mx-auto" style="max-width:520px;">
          <h2 class="font-serif mb-3">Logos</h2>
          <p class="text-muted">Configura logo principal y logo especifico para facturas.</p>
          <form method="post" enctype="multipart/form-data">
            <input type="hidden" name="action" value="main_logo"/>
            <label class="form-label fw-semibold">Logo principal (panel y liquidaciones)</label>
            <input class="form-control mb-3" type="file" name="logo" accept=".png,.jpg,.jpeg" required/>
            <button class="btn btn-brand" type="submit">Subir logo principal</button>
          </form>
          <hr/>
          <form method="post" enctype="multipart/form-data">
            <input type="hidden" name="action" value="invoice_logo"/>
            <label class="form-label fw-semibold">Logo de factura</label>
            <input class="form-control mb-3" type="file" name="invoice_logo" accept=".png,.jpg,.jpeg" required/>
            <button class="btn btn-outline-gold" type="submit">Subir logo de factura</button>
          </form>
        </div>
        """,
    )


@app.get("/owners/<int:owner_id>/contract")
@login_required
@roles_allowed("administrador", "secretaria", "contadora")
def owner_contract_download(owner_id: int) -> Any:
    row = fetch_rows(
        "SELECT contract_filename, full_name FROM owners WHERE id = ?",
        (owner_id,),
    )
    if not row or not row[0]["contract_filename"]:
        flash("No hay documento de contrato para este propietario.")
        return redirect(url_for("owners"))
    fn = row[0]["contract_filename"]
    path = CONTRACTS_DIR / fn
    if not path.exists():
        flash("El archivo del contrato no se encontro.")
        return redirect(url_for("owners"))
    safe_name = secure_filename(row[0]["full_name"] or "contrato") + Path(fn).suffix
    return send_from_directory(
        CONTRACTS_DIR, fn, as_attachment=True, download_name=safe_name
    )


@app.route("/owners/<int:owner_id>/upload-contract", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "secretaria")
def owner_contract_upload(owner_id: int) -> Any:
    row = fetch_rows("SELECT id, full_name, contract_filename FROM owners WHERE id = ?", (owner_id,))
    if not row:
        flash("Propietario no encontrado.")
        return redirect(url_for("owners"))
    owner = row[0]
    if request.method == "POST":
        contract_file = request.files.get("contract_doc")
        if not contract_file or not contract_file.filename:
            flash("Selecciona un archivo PDF.")
            return redirect(url_for("owner_contract_upload", owner_id=owner_id))
        saved = _save_owner_contract(owner_id, contract_file)
        if not saved:
            flash("Solo se permiten archivos PDF.")
            return redirect(url_for("owner_contract_upload", owner_id=owner_id))
        execute(
            "UPDATE owners SET contract_filename = ? WHERE id = ?",
            (saved, owner_id),
        )
        flash("Documento de contrato subido correctamente.")
        return redirect(url_for("owners"))
    name = owner["full_name"] or f"Propietario #{owner_id}"
    return layout(
        "Subir contrato",
        f"""
        <div class="card-onstay p-4 mx-auto" style="max-width:480px;">
          <h3 class="font-serif mb-2">Documento de contrato</h3>
          <p class="text-muted small mb-3">Propietario: <strong>{html.escape(name)}</strong></p>
          <form method="post" enctype="multipart/form-data">
            <input class="form-control mb-2" type="file" name="contract_doc"
              accept=".pdf,application/pdf" required>
            <div class="d-flex gap-2">
              <button class="btn btn-brand" type="submit">Subir</button>
              <a class="btn btn-outline-secondary" href="/owners">Volver</a>
            </div>
          </form>
          <p class="small text-muted mt-3 mb-0">Solo archivos PDF.</p>
        </div>
        """,
    )


@app.route("/owners", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "secretaria")
def owners() -> str:
    if request.method == "POST":
        pct_raw = (request.form.get("owner_percentage") or "").strip()
        pct: float | None
        try:
            pct = float(pct_raw.replace(",", ".")) if pct_raw else None
        except ValueError:
            pct = None
        contract_start = (request.form.get("contract_start_date") or "").strip() or None
        contract_end = (request.form.get("contract_end_date") or "").strip() or None
        owner_id = execute_insert(
            """
            INSERT INTO owners(
                full_name, client_contract_id, email, bank_name, transaction_type,
                owner_property_address, owner_percentage, tax_id, phone,
                contract_start_date, contract_end_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["full_name"].strip(),
                (request.form.get("client_contract_id") or "").strip(),
                (request.form.get("email") or "").strip(),
                (request.form.get("bank_name") or "").strip(),
                (request.form.get("transaction_type") or "").strip(),
                (request.form.get("owner_property_address") or "").strip(),
                pct,
                (request.form.get("tax_id") or "").strip(),
                (request.form.get("phone") or "").strip(),
                contract_start,
                contract_end,
            ),
        )
        contract_file = request.files.get("contract_doc")
        if contract_file and contract_file.filename:
            saved = _save_owner_contract(owner_id, contract_file)
            if saved:
                execute(
                    "UPDATE owners SET contract_filename = ? WHERE id = ?",
                    (saved, owner_id),
                )
        flash("Propietario creado.")
        return redirect(url_for("owners"))
    rows = fetch_rows("SELECT * FROM owners ORDER BY id DESC")
    table_rows: list[str] = []
    for r in rows:
        pct = row_get(r, "owner_percentage")
        if pct == "":
            pct_disp = "—"
        else:
            try:
                pct_disp = f"{float(pct):g}%"
            except ValueError:
                pct_disp = pct + "%" if pct else "—"
        start = row_get(r, "contract_start_date") or ""
        end = row_get(r, "contract_end_date") or ""
        contract_disp = f"{start} - {end}" if (start and end) else (start or end or "—")
        cf = row_get(r, "contract_filename") or ""
        upload_url = f"/owners/{r['id']}/upload-contract"
        if cf:
            doc_cell = (
                f'<a class="btn btn-sm btn-outline-gold" href="/owners/{r["id"]}/contract" '
                f'title="Ver contrato">Ver</a> '
                f'<a class="btn btn-sm btn-outline-secondary ms-1" href="{upload_url}">Subir otro</a>'
            )
        else:
            doc_cell = f'<a class="btn btn-sm btn-success" href="{upload_url}">Subir</a>'
        table_rows.append(
            f"<tr><td>{r['id']}</td><td>{r['full_name']}</td>"
            f"<td>{row_get(r, 'client_contract_id') or '—'}</td>"
            f"<td class='small'>{contract_disp}</td>"
            f"<td>{row_get(r, 'email') or '—'}</td>"
            f"<td>{row_get(r, 'bank_name') or '—'}</td>"
            f"<td>{row_get(r, 'transaction_type') or '—'}</td>"
            f"<td class='small'>{row_get(r, 'owner_property_address') or '—'}</td>"
            f"<td>{doc_cell}</td>"
            f"<td>{pct_disp}</td></tr>"
        )
    table = "".join(table_rows)
    return layout(
        "Propietarios",
        f"""
        <div class="row g-4">
          <div class="col-xl-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Nuevo propietario</h3>
              <p class="small text-muted mb-3">Datos segun contrato y pago.</p>
              <form method="post" enctype="multipart/form-data">
                <label class="form-label fw-semibold">Nombre completo</label>
                <input class="form-control mb-2" name="full_name" placeholder="Nombre y apellidos" required>
                <label class="form-label fw-semibold">ID cliente (orden de contrato)</label>
                <input class="form-control mb-2" name="client_contract_id" placeholder="Ej. 001, C-2025-01">
                <label class="form-label fw-semibold">Fecha contrato (inicio - vencimiento)</label>
                <div class="row g-2 mb-2">
                  <div class="col-6">
                    <input class="form-control" type="date" name="contract_start_date" placeholder="Fecha inicio">
                  </div>
                  <div class="col-6">
                    <input class="form-control" type="date" name="contract_end_date" placeholder="Fecha vencimiento">
                  </div>
                </div>
                <label class="form-label fw-semibold">Correo</label>
                <input class="form-control mb-2" type="email" name="email" placeholder="correo@ejemplo.com">
                <label class="form-label fw-semibold">Banco</label>
                <input class="form-control mb-2" name="bank_name" placeholder="Nombre del banco">
                <label class="form-label fw-semibold">Tipo de transaccion</label>
                <select class="form-select mb-2" name="transaction_type">
                  <option value="">Selecciona...</option>
                  <option value="transferencia">Transferencia bancaria</option>
                  <option value="deposito">Deposito</option>
                  <option value="cheque">Cheque</option>
                  <option value="efectivo">Efectivo</option>
                  <option value="zelle">Zelle</option>
                  <option value="ach">ACH / wire</option>
                  <option value="otro">Otro</option>
                </select>
                <label class="form-label fw-semibold">Direccion de propiedad</label>
                <textarea class="form-control mb-2" name="owner_property_address" rows="2"
                  placeholder="Calle, numero, sector, ciudad"></textarea>
                <label class="form-label fw-semibold">Porcentaje (%)</label>
                <input class="form-control mb-2" type="number" step="0.01" name="owner_percentage"
                  placeholder="Ej. 20 (comision de gestion o referencia)">
                <hr class="my-3">
                <p class="small text-muted mb-2">Opcional</p>
                <input class="form-control mb-2" name="tax_id" placeholder="ID fiscal (RNC / cedula)">
                <input class="form-control mb-2" name="phone" placeholder="Telefono">
                <label class="form-label fw-semibold">Documento de contrato PDF (opcional)</label>
                <input class="form-control mb-3" type="file" name="contract_doc"
                  accept=".pdf,application/pdf" title="Solo PDF">
                <button class="btn btn-brand">Guardar propietario</button>
              </form>
            </div>
          </div>
          <div class="col-xl-7">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Listado</h3>
              <div class="table-responsive" style="max-height:70vh;">
                <table class="table table-hover table-sm align-middle mb-0">
                  <thead class="table-light sticky-top">
                    <tr>
                      <th>ID</th><th>Nombre</th><th>ID cliente</th><th>Contrato</th><th>Correo</th>
                      <th>Banco</th><th>Tipo pago</th><th>Direccion</th><th>Documento</th><th>%</th>
                    </tr>
                  </thead>
                  <tbody>{table or '<tr><td colspan="10" class="text-muted">Sin datos</td></tr>'}</tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        """,
    )


@app.route("/properties", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "secretaria")
def properties() -> str:
    owners_list = fetch_rows(
        "SELECT id, full_name, owner_percentage FROM owners ORDER BY full_name"
    )
    if request.method == "POST":
        oid = int(request.form["owner_id"])
        comm_raw = (request.form.get("commission_pct") or "").strip()
        if comm_raw:
            comm = float(comm_raw.replace(",", "."))
        else:
            ow = fetch_rows(
                "SELECT owner_percentage FROM owners WHERE id = ?", (oid,)
            )
            op = ow[0]["owner_percentage"] if ow else None
            comm = float(op) if op is not None else 20.0
        execute(
            """
            INSERT INTO properties(
                name, address, owner_id, commission_pct, cleaning_fee_default,
                airbnb_ical_url, booking_ical_url, access_security_info
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["name"],
                request.form["address"],
                oid,
                comm,
                float(request.form.get("cleaning_fee_default", 0)),
                request.form.get("airbnb_ical_url", ""),
                request.form.get("booking_ical_url", ""),
                (request.form.get("access_security_info") or "").strip() or None,
            ),
        )
        flash("Propiedad creada.")
        return redirect(url_for("properties"))
    rows = fetch_rows(
        """
        SELECT p.*, o.full_name AS owner_name
        FROM properties p
        JOIN owners o ON o.id = p.owner_id
        ORDER BY p.id DESC
        """
    )
    owner_options = "".join(
        f"<option value='{o['id']}'>{o['full_name']}</option>" for o in owners_list
    )
    table = "".join(
        f"<tr><td>{r['name']}</td><td>{r['owner_name']}</td><td>{r['commission_pct']}%</td>"
        f"<td><a href='/sync-ical/{r['id']}' class='btn btn-sm btn-outline-gold'>Sync iCal</a></td></tr>"
        for r in rows
    )
    return layout(
        "Propiedades",
        f"""
        <div class="row g-4">
          <div class="col-lg-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Nueva propiedad</h3>
              <form method="post">
                <input class="form-control mb-2" name="name" placeholder="Nombre / apto" required>
                <input class="form-control mb-2" name="address" placeholder="Direccion completa">
                <select class="form-select mb-2" name="owner_id" required>
                  <option value="">Propietario</option>
                  {owner_options}
                </select>
                <input class="form-control mb-2" type="number" step="0.01" name="commission_pct"
                  placeholder="Comision % (si vacio, usa el % del propietario o 20)">
                <input class="form-control mb-2" type="number" step="0.01" name="cleaning_fee_default" placeholder="Tarifa limpieza default" value="0">
                <input class="form-control mb-2" name="airbnb_ical_url" placeholder="URL iCal Airbnb">
                <input class="form-control mb-2" name="booking_ical_url" placeholder="URL iCal Booking">
                <label class="form-label small text-danger">Seguridad (opcional, confidencial)</label>
                <textarea class="form-control mb-3" name="access_security_info" rows="2"
                  placeholder="Clave de puerta, codigo, ubicacion de llave..."></textarea>
                <button class="btn btn-brand">Guardar</button>
              </form>
            </div>
          </div>
          <div class="col-lg-7">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Inventario</h3>
              <div class="table-responsive">
                <table class="table table-hover align-middle mb-0">
                  <thead class="table-light"><tr><th>Propiedad</th><th>Propietario</th><th>Comision</th><th>iCal</th></tr></thead>
                  <tbody>{table or '<tr><td colspan="4" class="text-muted">Sin datos</td></tr>'}</tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        """,
    )


@app.route("/security", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "secretaria")
def security_access() -> str:
    if request.method == "POST":
        pid = int(request.form["property_id"])
        info = (request.form.get("access_security_info") or "").strip()
        execute(
            "UPDATE properties SET access_security_info = ? WHERE id = ?",
            (info or None, pid),
        )
        flash("Datos de seguridad actualizados.")
        return redirect(url_for("security_access"))
    rows = fetch_rows(
        """
        SELECT p.id, p.name, p.address, p.access_security_info, o.full_name AS owner_name
        FROM properties p
        JOIN owners o ON o.id = p.owner_id
        ORDER BY o.full_name, p.name
        """
    )
    cards: list[str] = []
    for r in rows:
        raw = row_get(r, "access_security_info")
        safe_ta = html.escape(raw)
        addr = html.escape(row_get(r, "address") or "—")
        cards.append(
            f"""
            <div class="card-onstay p-4 mb-3 border-start border-4 border-danger border-opacity-25">
              <div class="row g-3">
                <div class="col-md-4">
                  <div class="text-muted small text-uppercase">Propietario</div>
                  <div class="fw-semibold">{html.escape(r['owner_name'])}</div>
                  <div class="text-muted small text-uppercase mt-2">Propiedad</div>
                  <div class="fw-semibold">{html.escape(r['name'])}</div>
                  <div class="text-muted small text-uppercase mt-2">Direccion</div>
                  <div class="small">{addr}</div>
                </div>
                <div class="col-md-8">
                  <form method="post">
                    <input type="hidden" name="property_id" value="{r['id']}">
                    <label class="form-label fw-semibold text-danger">Clave de puerta / codigo / llave</label>
                    <textarea class="form-control font-monospace mb-2" name="access_security_info"
                      rows="4" placeholder="Ej. Codigo 1234# / Llave caja fuerte recepcion / Smart lock...">{safe_ta}</textarea>
                    <button class="btn btn-brand btn-sm" type="submit">Guardar esta propiedad</button>
                  </form>
                </div>
              </div>
            </div>
            """
        )
    body = "".join(cards) or "<p class='text-muted'>No hay propiedades. Crea una en Propiedades.</p>"
    return layout(
        "Seguridad y acceso",
        f"""
        <div class="alert alert-danger border-0 shadow-sm mb-4">
          <strong>Confidencial.</strong> Solo administracion y secretaria ven esta ventana.
          No compartas claves por WhatsApp o correo sin cifrado. Los datos se guardan en tu base local (<code>onstay.db</code>).
        </div>
        <h2 class="font-serif mb-4">Acceso a unidades</h2>
        {body}
        """,
    )


@app.get("/sync-ical/<int:property_id>")
@login_required
@roles_allowed("administrador", "secretaria")
def sync_ical(property_id: int) -> Any:
    prop = fetch_rows("SELECT * FROM properties WHERE id = ?", (property_id,))
    if not prop:
        flash("Propiedad no encontrada.")
        return redirect(url_for("properties"))
    property_row = prop[0]
    imported = 0
    for source, url in [
        ("airbnb", property_row["airbnb_ical_url"]),
        ("booking", property_row["booking_ical_url"]),
    ]:
        if not url:
            continue
        try:
            data = requests.get(url, timeout=15).text
            cal = Calendar.from_ical(data)
            for component in cal.walk():
                if component.name != "VEVENT":
                    continue
                uid = str(component.get("uid", ""))
                checkin = component.decoded("dtstart").strftime("%Y-%m-%d")
                checkout = component.decoded("dtend").strftime("%Y-%m-%d")
                guest = str(component.get("summary", "Reserva iCal"))
                with db() as conn:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO reservations(
                            property_id, guest_name, source, checkin, checkout,
                            checkin_time, checkout_time, cleaning_in_charge_name,
                            gross_amount, bank_cost, cleaning_fee, general_cost, notes, external_uid
                        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, 0, 0, ?, 0, ?, ?)
                        """,
                        (
                            property_id,
                            guest,
                            source,
                            checkin,
                            checkout,
                            property_row["cleaning_fee_default"],
                            "Reserva importada desde iCal",
                            uid,
                        ),
                    )
                    if conn.total_changes > 0:
                        imported += 1
        except Exception as exc:
            flash(f"Error sincronizando {source}: {exc}")
    flash(f"Sincronizacion finalizada. Nuevas reservas: {imported}")
    return redirect(url_for("reservations"))


@app.route("/reservations", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "secretaria")
def reservations() -> str:
    properties_list = fetch_rows("SELECT id, name FROM properties ORDER BY name")
    if request.method == "POST":
        cin_t = (request.form.get("checkin_time") or "").strip()
        cout_t = (request.form.get("checkout_time") or "").strip()
        enc = (request.form.get("cleaning_in_charge_name") or "").strip()
        execute(
            """
            INSERT INTO reservations(
                property_id, guest_name, source, checkin, checkout,
                checkin_time, checkout_time, cleaning_in_charge_name,
                gross_amount, bank_cost, cleaning_fee, general_cost, notes, external_uid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["property_id"],
                request.form["guest_name"],
                request.form["source"],
                request.form["checkin"],
                request.form["checkout"],
                cin_t or None,
                cout_t or None,
                enc or None,
                float(request.form["gross_amount"]),
                float(request.form.get("bank_cost", 0)),
                float(request.form.get("cleaning_fee", 0)),
                float(request.form.get("general_cost", 0)),
                request.form.get("notes", ""),
                "",
            ),
        )
        flash("Reserva registrada.")
        return redirect(url_for("reservations"))
    rows = fetch_rows(
        """
        SELECT r.*, p.name as property_name,
          (SELECT t.cleaner_name FROM cleaning_tasks t
           WHERE t.reservation_id = r.id ORDER BY t.id DESC LIMIT 1) AS last_task_cleaner
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        ORDER BY r.checkin DESC, r.id DESC
        LIMIT 100
        """
    )
    options = "".join(f"<option value='{p['id']}'>{p['name']}</option>" for p in properties_list)
    table_rows: list[str] = []
    for r in rows:
        cin_disp = format_date_time(row_get(r, "checkin"), row_get(r, "checkin_time") or None)
        cout_disp = format_date_time(row_get(r, "checkout"), row_get(r, "checkout_time") or None)
        enc_res = (row_get(r, "cleaning_in_charge_name") or "").strip()
        enc_task = (row_get(r, "last_task_cleaner") or "").strip()
        enc_show = enc_res or enc_task or "—"
        table_rows.append(
            f"<tr><td class='text-nowrap'>{cin_disp}</td><td class='text-nowrap'>{cout_disp}</td>"
            f"<td>{r['property_name']}</td><td>{r['guest_name']}</td><td>{r['source']}</td>"
            f"<td>${r['gross_amount']:.2f}</td><td>{enc_show}</td></tr>"
        )
    table = "".join(table_rows)
    return layout(
        "Reservas",
        f"""
        <div class="row g-4">
          <div class="col-lg-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Nueva reserva</h3>
              <form method="post">
                <select class="form-select mb-2" name="property_id" required>
                  <option value="">Propiedad</option>{options}
                </select>
                <input class="form-control mb-2" name="guest_name" placeholder="Nombre cliente" required>
                <select class="form-select mb-2" name="source">
                  <option value="airbnb">Airbnb</option><option value="booking">Booking</option><option value="manual">Manual</option>
                </select>
                <label class="form-label small mb-0">Check-in</label>
                <div class="row g-2 mb-2">
                  <div class="col-7"><input class="form-control" type="date" name="checkin" required></div>
                  <div class="col-5"><input class="form-control" type="time" name="checkin_time" title="Hora check-in"></div>
                </div>
                <label class="form-label small mb-0">Check-out</label>
                <div class="row g-2 mb-2">
                  <div class="col-7"><input class="form-control" type="date" name="checkout" required></div>
                  <div class="col-5"><input class="form-control" type="time" name="checkout_time" title="Hora check-out"></div>
                </div>
                <label class="form-label small">Encargada de limpieza (esta reserva)</label>
                <input class="form-control mb-2" name="cleaning_in_charge_name" placeholder="Nombre de quien limpia / supervisa">
                <input class="form-control mb-2" type="number" step="0.01" name="gross_amount" placeholder="Monto reserva USD" required>
                <input class="form-control mb-2" type="number" step="0.01" name="bank_cost" placeholder="Costos bancarios">
                <input class="form-control mb-2" type="number" step="0.01" name="cleaning_fee" placeholder="Tarifa limpieza">
                <input class="form-control mb-2" type="number" step="0.01" name="general_cost" placeholder="Gastos generales">
                <textarea class="form-control mb-3" name="notes" placeholder="Notas"></textarea>
                <button class="btn btn-brand">Guardar</button>
              </form>
            </div>
          </div>
          <div class="col-lg-7">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Ultimas reservas</h3>
              <div class="table-responsive">
                <table class="table table-hover table-sm align-middle mb-0">
                  <thead class="table-light"><tr><th>Check-in</th><th>Check-out</th><th>Propiedad</th><th>Cliente</th><th>Canal</th><th>Monto</th><th>Encargada limpieza</th></tr></thead>
                  <tbody>{table or '<tr><td colspan="7" class="text-muted">Sin datos</td></tr>'}</tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        """,
    )


@app.route("/cleaning", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "secretaria")
def cleaning() -> str:
    if request.method == "POST":
        phone = request.form.get("cleaner_phone", "") or ""
        stime = request.form.get("scheduled_time", "") or ""
        key_note = (request.form.get("cleaning_key_note") or "").strip()
        execute(
            """
            INSERT INTO cleaning_tasks(
                reservation_id, cleaner_name, cleaner_phone, scheduled_date, scheduled_time,
                status, notes, cleaning_key_note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.form["reservation_id"],
                request.form["cleaner_name"],
                phone,
                request.form["scheduled_date"],
                stime,
                request.form["status"],
                request.form.get("notes", ""),
                key_note or None,
            ),
        )
        flash("Tarea de limpieza creada.")
        return redirect(url_for("cleaning"))
    reservations_list = fetch_rows(
        """
        SELECT r.id, r.guest_name, r.checkin, r.checkout, p.name AS prop_name, p.address AS prop_address
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        ORDER BY r.checkin DESC LIMIT 100
        """
    )
    rows = fetch_rows(
        """
        SELECT c.*, r.guest_name, r.checkin, r.checkout, r.checkin_time, r.checkout_time,
               p.name AS property_name, p.address AS property_address, p.access_security_info
        FROM cleaning_tasks c
        JOIN reservations r ON r.id = c.reservation_id
        JOIN properties p ON p.id = r.property_id
        ORDER BY c.scheduled_date DESC, c.id DESC
        """
    )
    options = "".join(
        f"<option value='{r['id']}'>#{r['id']} {r['prop_name']} - {r['guest_name']} ({r['checkin']})</option>"
        for r in reservations_list
    )
    wa_company = get_setting("company_phone_whatsapp", "").strip()
    wa_company_disp = wa_company
    if wa_company and not wa_company.startswith("+"):
        wa_company_disp = f"+{wa_company}"
    table_rows = []
    for t in rows:
        addr = t["property_address"] or "Sin direccion"
        prop = t["property_name"]
        guest = t["guest_name"]
        cin_disp = format_date_time(row_get(t, "checkin"), row_get(t, "checkin_time") or None)
        cout_disp = format_date_time(row_get(t, "checkout"), row_get(t, "checkout_time") or None)
        fecha = t["scheduled_date"]
        hora = t["scheduled_time"] or "Por confirmar"
        cleaner = t["cleaner_name"]
        phone = t["cleaner_phone"] or ""
        access_prop = row_get(t, "access_security_info")
        key_note = row_get(t, "cleaning_key_note")
        acc_parts = []
        if access_prop.strip():
            acc_parts.append(access_prop.strip())
        if key_note.strip():
            acc_parts.append(f"Llavero/nota: {key_note.strip()}")
        acc_full = " | ".join(acc_parts) if acc_parts else ""
        acc_preview = (acc_full[:80] + "…") if len(acc_full) > 80 else (acc_full or "—")
        acc_title_attr = ""
        if acc_full:
            acc_title_attr = html.escape(
                acc_full.replace("\n", " ").replace("\r", "")[:400],
                quote=True,
            )
        acc_cell = (
            f'<span class="small" title="{acc_title_attr}">{html.escape(acc_preview)}</span>'
            if acc_full
            else "<span class='small text-muted'>—</span>"
        )
        msg = build_cleaning_whatsapp_message(
            cleaner,
            prop,
            addr,
            fecha,
            hora,
            cin_disp,
            cout_disp,
            guest,
            access_prop,
            key_note,
            row_get(t, "notes"),
        )
        wa = whatsapp_url(phone, msg) if phone else "#"
        web_btn = (
            '<a class="btn btn-sm btn-outline-dark mb-1" '
            'href="https://web.whatsapp.com/" title="Inicia sesion con el WhatsApp de la empresa">'
            "WhatsApp Web empresa</a>"
        )
        if phone:
            send_btn = (
                f'<a class="btn btn-sm btn-success" href="{wa}">'
                "Enviar recordatorio</a>"
            )
        else:
            send_btn = '<span class="small text-muted">Agrega telefono del personal</span>'
        wa_cell = f'<div class="d-flex flex-column align-items-start gap-1">{web_btn}{send_btn}</div>'
        table_rows.append(
            f"<tr><td>{fecha}<br/><small class='text-muted'>{hora}</small></td>"
            f"<td>{html.escape(prop)}</td><td>{acc_cell}</td><td>{html.escape(cleaner)}</td>"
            f"<td>{t['status']}</td><td>{wa_cell}</td></tr>"
        )
    table = "".join(table_rows)
    return layout(
        "Limpieza",
        f"""
        <div class="alert alert-warning border-0 small mb-4">
          <strong>WhatsApp desde el numero de la empresa:</strong> abre primero
          <strong>WhatsApp Web empresa</strong> e inicia sesion con el telefono de OnstayRd
          {f"({html.escape(wa_company_disp)})" if wa_company else ""}.
          Luego pulsa <strong>Enviar recordatorio</strong>: se abrira el chat del personal con el mensaje
          listo (incluye clave/acceso del apto desde <a href="/security">Seguridad</a>).
          El envio final lo confirmas tu en WhatsApp (no se envia solo sin tu clic).
        </div>
        <div class="row g-4">
          <div class="col-lg-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-2">Nueva tarea</h3>
              <p class="small text-muted">La clave del apto se toma de <a href="/security">Seguridad</a> y se incluye en el mensaje.</p>
              <form method="post">
                <select class="form-select mb-2" name="reservation_id" required>
                  <option value="">Reserva / propiedad</option>{options}
                </select>
                <input class="form-control mb-2" name="cleaner_name" placeholder="Nombre de quien limpia" required>
                <input class="form-control mb-2" name="cleaner_phone" placeholder="WhatsApp del personal (ej. 8294755974)">
                <label class="form-label small">Numero de llave / llavero / caja (opcional)</label>
                <input class="form-control mb-2" name="cleaning_key_note" placeholder="Ej. Llavero 12-B, caja recepcion">
                <div class="row g-2">
                  <div class="col-md-6"><input class="form-control mb-2" type="date" name="scheduled_date" required></div>
                  <div class="col-md-6"><input class="form-control mb-2" type="time" name="scheduled_time" placeholder="Hora"></div>
                </div>
                <select class="form-select mb-2" name="status">
                  <option value="pending">Pendiente</option>
                  <option value="in_progress">En progreso</option>
                  <option value="done">Hecha</option>
                </select>
                <textarea class="form-control mb-3" name="notes" placeholder="Notas extra (estacionamiento, alarma, etc.)"></textarea>
                <button class="btn btn-brand">Guardar</button>
              </form>
            </div>
          </div>
          <div class="col-lg-7">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Plan de limpieza</h3>
              <div class="card bg-light border mb-3 p-3">
                <div class="small fw-semibold text-dark mb-2">WhatsApp (siempre visible)</div>
                <div class="d-flex flex-wrap gap-2 align-items-center">
                  <a class="btn btn-outline-dark btn-sm" href="https://web.whatsapp.com/">1. WhatsApp Web empresa</a>
                  <span class="small text-muted">Abre Web e inicia sesion con el telefono de OnstayRd.</span>
                </div>
                <hr class="my-2">
                <p class="small text-muted mb-0">
                  <strong>2. Enviar recordatorio</strong> aparece en cada <strong>fila de abajo</strong> cuando existe una tarea
                  y pusiste el <strong>WhatsApp del personal</strong>. Si no hay filas, crea una tarea a la izquierda.
                  Usuario <strong>contadora</strong> no ve esta pagina; entra como <strong>admin</strong> o <strong>secretaria</strong>.
                </p>
              </div>
              <div class="table-responsive">
                <table class="table table-hover table-sm align-middle mb-0">
                  <thead class="table-light">
                    <tr>
                      <th>Fecha</th><th>Propiedad</th><th>Clave / llave apto</th>
                      <th>Responsable</th><th>Estado</th><th>Recordatorio</th>
                    </tr>
                  </thead>
                  <tbody>{table or '''<tr><td colspan="6" class="p-4">
                    <p class="text-muted mb-2"><strong>Aun no hay tareas de limpieza.</strong></p>
                    <p class="small text-muted mb-0">Completa el formulario de la izquierda (reserva, nombre del personal,
                    <strong>WhatsApp del personal</strong>, fecha) y pulsa Guardar. Entonces veras aqui los botones
                    <em>WhatsApp Web empresa</em> (por fila) y <em>Enviar recordatorio</em>.</p>
                  </td></tr>'''}</tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        """,
    )


def settlement_rows(month: str) -> list[sqlite3.Row]:
    start, end = parse_month(month)
    return fetch_rows(
        """
        SELECT
          o.id as owner_id,
          o.full_name as owner_name,
          p.name as property_name,
          p.id as property_id,
          r.checkin,
          r.guest_name,
          r.gross_amount,
          r.bank_cost,
          r.cleaning_fee,
          r.general_cost,
          p.commission_pct,
          (r.gross_amount - r.bank_cost - r.cleaning_fee - r.general_cost) AS owner_benefit
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        JOIN owners o ON o.id = p.owner_id
        WHERE r.checkin >= ? AND r.checkin < ?
        ORDER BY o.full_name, p.name, r.checkin
        """,
        (start, end),
    )


def settlement_admin_effective_pct(rows: list[sqlite3.Row]) -> float:
    total_b = sum(float(r["owner_benefit"] or 0) for r in rows)
    if total_b <= 0:
        return 0.0
    comm = sum(
        float(r["owner_benefit"] or 0) * float(r["commission_pct"] or 0) / 100.0 for r in rows
    )
    return 100.0 * comm / total_b


def owner_period_cleaning_total(owner_id: int, month: str) -> float:
    start, end = parse_month(month)
    row = fetch_rows(
        """
        SELECT COALESCE(SUM(r.cleaning_fee), 0) AS t
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        WHERE p.owner_id = ? AND r.checkin >= ? AND r.checkin < ?
        """,
        (owner_id, start, end),
    )
    return float(row[0]["t"] or 0) if row else 0.0


def owner_period_maintenance_total(owner_id: int, month: str) -> float:
    row = fetch_rows(
        """
        SELECT COALESCE(SUM(amount), 0) AS t
        FROM maintenance_billing
        WHERE owner_id = ? AND month = ?
        """,
        (owner_id, month),
    )
    return float(row[0]["t"] or 0) if row else 0.0


def invoice_extra_amounts(owner_id: int, month: str) -> tuple[float, float]:
    return owner_period_cleaning_total(owner_id, month), owner_period_maintenance_total(
        owner_id, month
    )


def settlement_rows_owner(month: str, owner_id: int) -> list[sqlite3.Row]:
    start, end = parse_month(month)
    return fetch_rows(
        """
        SELECT
          o.id as owner_id,
          o.full_name as owner_name,
          p.name as property_name,
          r.checkin,
          r.guest_name,
          r.gross_amount,
          r.bank_cost,
          r.cleaning_fee,
          r.general_cost,
          p.commission_pct,
          (r.gross_amount - r.bank_cost - r.cleaning_fee - r.general_cost) AS owner_benefit
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        JOIN owners o ON o.id = p.owner_id
        WHERE r.checkin >= ? AND r.checkin < ? AND o.id = ?
        ORDER BY p.name, r.checkin
        """,
        (start, end, owner_id),
    )


def _logo_data_uri_for_pdf() -> str | None:
    for candidate, mime in (
        ("logo.png", "image/png"),
        ("logo.jpg", "image/jpeg"),
        ("logo.jpeg", "image/jpeg"),
    ):
        logo_path = UPLOAD_DIR / candidate
        if logo_path.exists():
            b64 = base64.b64encode(logo_path.read_bytes()).decode("ascii")
            return f"data:{mime};base64,{b64}"
    return None


# Estilos para PDF (xhtml2pdf): sin flex ni @import; colores iguales a la vista web
PDF_LIQUIDACION_CSS = """
<style>
  @page { size: a4 portrait; margin: 14mm; }
  body {
    margin: 0; padding: 0; background: #faf7f2; font-family: Helvetica, Arial, sans-serif;
    color: #2c2618; font-size: 10pt;
  }
  .doc {
    background: #fff; border: 1px solid #e6dfcf; padding: 28px 32px; margin: 0 auto;
  }
  .hdr-sep { border-bottom: 2px solid #9e7f44; margin: 16px 0 20px 0; padding-top: 4px; }
  .title-doc {
    font-family: Georgia, 'Times New Roman', serif; font-size: 22pt; font-weight: 600;
    letter-spacing: 0.08em; text-align: center; margin: 0; color: #2c2618; line-height: 1.1;
  }
  .subtitle { text-align: center; font-size: 11pt; color: #6b6356; margin-top: 8px; }
  .meta-table { width: 100%; margin: 16px 0; font-size: 10pt; border-collapse: collapse; }
  .meta-table td { border-bottom: 1px solid #e6dfcf; padding: 8px 6px; vertical-align: top; }
  .meta-label { width: 140px; font-weight: bold; color: #9e7f44; }
  table.data { width: 100%; border-collapse: collapse; font-size: 9pt; margin-top: 10px; }
  table.data th {
    background: #f2e9de; color: #4a402e; padding: 10px 8px; text-align: left;
    border: 1px solid #e6dfcf; font-weight: bold;
  }
  table.data td { padding: 8px; border: 1px solid #e6dfcf; vertical-align: top; }
  .total-row td { background: #f2e9de; font-weight: bold; color: #2c2618; }
  .section-title {
    font-family: Georgia, 'Times New Roman', serif; font-size: 12pt; color: #9e7f44;
    margin: 22px 0 8px 0; font-weight: 600;
  }
  .footer-doc { margin-top: 24px; font-size: 8.5pt; color: #6b6356; text-align: center; line-height: 1.4; }
  .sig-table { width: 100%; margin-top: 36px; border-top: 1px solid #e6dfcf; padding-top: 20px; border-collapse: collapse; }
  .sig-table td { width: 50%; vertical-align: top; padding: 8px 16px 8px 0; font-size: 9.5pt; }
  .sig-line { border-top: 1px solid #2c2618; margin-top: 44px; padding-top: 8px; }
  .logo-cell { width: 108px; vertical-align: top; padding-right: 12px; }
  .logo-img { width: 100px; height: 100px; border-radius: 50px; background: #f2e9de; padding: 8px; }
</style>
"""


PRINT_DOC_CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@500;600;700&family=Source+Sans+3:wght@400;600&display=swap');
  @page { margin: 16mm; }
  body { font-family: 'Source Sans 3', sans-serif; color: #2c2618; background: #faf7f2; margin: 0; padding: 24px; }
  .doc { max-width: 800px; margin: 0 auto; background: #fff; border: 1px solid #e6dfcf; padding: 32px 36px; border-radius: 4px; }
  .doc-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 24px; border-bottom: 2px solid #9e7f44; padding-bottom: 20px; margin-bottom: 24px; }
  .logo-doc { width: 100px; height: 100px; object-fit: contain; border-radius: 50%; background: #f2e9de; padding: 8px; }
  .title-doc { font-family: 'Cormorant Garamond', serif; font-size: 2rem; letter-spacing: .08em; text-align: center; flex: 1; margin: 0; color: #2c2618; }
  .subtitle { text-align: center; font-size: 1.05rem; color: #6b6356; margin-top: 4px; }
  .meta { margin: 20px 0; font-size: 0.95rem; }
  .meta-row { display: flex; border-bottom: 1px solid #e6dfcf; padding: 6px 0; }
  .meta-label { width: 140px; font-weight: 600; color: #9e7f44; }
  table.data { width: 100%; border-collapse: collapse; font-size: 0.88rem; margin-top: 8px; }
  table.data th { background: #f2e9de; color: #4a402e; padding: 10px 8px; text-align: left; border: 1px solid #e6dfcf; font-weight: 600; }
  table.data td { padding: 8px; border: 1px solid #e6dfcf; vertical-align: top; }
  .total-row { background: #f2e9de; font-weight: 700; }
  .signatures { display: flex; gap: 40px; margin-top: 48px; padding-top: 24px; border-top: 1px solid #e6dfcf; }
  .sig { flex: 1; font-size: 0.9rem; }
  .sig-line { border-top: 1px solid #2c2618; margin-top: 48px; padding-top: 8px; }
  .footer-doc { margin-top: 28px; font-size: 0.8rem; color: #6b6356; text-align: center; }
  @media print {
    body { background: #fff; padding: 0; }
    .no-print { display: none !important; }
    .doc { border: none; box-shadow: none; }
  }
</style>
"""




def _liquidacion_plantilla_context(month: str, owner_id: int | None) -> dict[str, Any]:
    if owner_id:
        rows = settlement_rows_owner(month, owner_id)
        owner_name = fetch_rows("SELECT full_name FROM owners WHERE id = ?", (owner_id,))
        oname = owner_name[0]["full_name"] if owner_name else ""
        prop_display = rows[0]["property_name"] if rows else "—"
    else:
        rows = settlement_rows(month)
        oname = "Todos los propietarios"
        prop_display = "Varias" if rows else "—"
    total = sum(r["owner_benefit"] for r in rows)
    admin_eff = settlement_admin_effective_pct(rows)
    lines = "".join(
        f"<tr><td>{r['checkin']}</td><td>{html.escape(str(r['guest_name'] or ''))}</td>"
        f"<td>${float(r['gross_amount']):.2f}</td><td>${float(r['bank_cost']):.2f}</td>"
        f"<td>${float(r['cleaning_fee']):.2f}</td><td>${float(r['general_cost']):.2f}</td>"
        f"<td>{float(r['commission_pct'] or 0):.2f}%</td>"
        f"<td>${float(r['owner_benefit']):.2f}</td></tr>"
        for r in rows
    )
    company = get_setting("company_name")
    email = get_setting("company_email")
    phone = get_setting("company_phone")
    ml = month_label_es(month)
    legal = get_setting("legal_note")
    return {
        "month": month,
        "owner_id": owner_id,
        "rows": rows,
        "oname": oname,
        "prop_display": prop_display,
        "total": total,
        "admin_eff": admin_eff,
        "lines_html": lines,
        "company": company,
        "email": email,
        "phone": phone,
        "ml": ml,
        "legal": legal,
    }


def _liquidacion_plantilla_pdf_html(ctx: dict[str, Any]) -> str:
    c = html.escape(str(ctx.get("company") or ""))
    ml = html.escape(str(ctx.get("ml") or ""))
    on = html.escape(str(ctx.get("oname") or ""))
    pd = html.escape(str(ctx.get("prop_display") or ""))
    em = html.escape(str(ctx.get("email") or ""))
    ph = html.escape(str(ctx.get("phone") or ""))
    leg = html.escape(str(ctx.get("legal") or ""))
    det = ctx["lines_html"] or (
        '<tr><td colspan="8" style="text-align:center;">Sin movimientos en este periodo</td></tr>'
    )
    tot = ctx["total"]
    ae = ctx["admin_eff"]
    logo_uri = _logo_data_uri_for_pdf()
    logo_block = (
        f'<img class="logo-img" src="{logo_uri}" alt=""/>'
        if logo_uri
        else '<div class="logo-img">&nbsp;</div>'
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"/>{PDF_LIQUIDACION_CSS}</head><body>
<div class="doc">
  <table width="100%" cellpadding="0" cellspacing="0" border="0">
    <tr>
      <td class="logo-cell">{logo_block}</td>
      <td align="center" valign="middle">
        <div class="title-doc">OnstayRd</div>
        <div class="subtitle">Factura mensual de reservas</div>
      </td>
      <td style="width:100px;">&nbsp;</td>
    </tr>
  </table>
  <div class="hdr-sep"></div>
  <table class="meta-table">
    <tr><td class="meta-label">Mes</td><td>{ml}</td></tr>
    <tr><td class="meta-label">Propiedad</td><td>{pd}</td></tr>
    <tr><td class="meta-label">Propietario</td><td>{on}</td></tr>
    <tr><td class="meta-label">% adm. efectivo</td><td>{ae:.2f}% (ponderado sobre beneficio del periodo)</td></tr>
  </table>
  <table class="data">
    <thead><tr>
      <th>Fecha reserva</th><th>Nombre cliente</th><th>Monto reserva</th><th>Costos bancarios</th>
      <th>Tarifa limpieza</th><th>Gastos generales</th><th>% adm.</th><th>Beneficio propietario</th>
    </tr></thead>
    <tbody>{det}
    <tr class="total-row"><td colspan="7" style="text-align:right;">TOTAL PROPIETARIO (USD)</td><td>${tot:,.2f}</td></tr>
    </tbody>
  </table>
  <table class="sig-table">
    <tr>
      <td>
        <strong>Firma Propietario</strong>
        <div class="sig-line">Nombre: __________________ &nbsp; Fecha: __________</div>
      </td>
      <td>
        <strong>Firma Administrador</strong>
        <p style="margin:4px 0;">{c}</p>
        <div class="sig-line">Fecha: __________</div>
      </td>
    </tr>
  </table>
  <div class="footer-doc">{leg}<br/>{em} &middot; {ph}</div>
</div>
</body></html>"""


def _liquidacion_plantilla_pdf_bytes(ctx: dict[str, Any]) -> bytes:
    from io import StringIO

    from xhtml2pdf import pisa

    out = io.BytesIO()
    result = pisa.CreatePDF(StringIO(_liquidacion_plantilla_pdf_html(ctx)), dest=out, encoding="utf-8")
    if result.err:
        raise RuntimeError("Fallo al generar PDF")
    return out.getvalue()


@app.get("/download/liquidacion")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def download_liquidacion() -> Any:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    owner_id = request.args.get("owner_id", type=int)
    ctx = _liquidacion_plantilla_context(month, owner_id)
    try:
        pdf_data = _liquidacion_plantilla_pdf_bytes(ctx)
    except Exception as e:
        traceback.print_exc()
        flash(
            f"No se pudo generar el PDF: {str(e) or 'error desconocido'}. "
            "Revisa la terminal donde corre la app para ver el detalle."
        )
        return redirect(url_for("settlements", month=month))
    if owner_id and ctx["oname"]:
        safe = secure_filename(ctx["oname"].replace(" ", "_"))[:40] or "propietario"
        fname = f"liquidacion_{month}_{safe}.pdf"
    else:
        fname = f"liquidacion_{month}_todos.pdf"
    return send_file(
        io.BytesIO(pdf_data),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname,
    )


@app.get("/print/liquidacion")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def print_liquidacion() -> str:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    owner_id = request.args.get("owner_id", type=int)
    owners_opts = fetch_rows("SELECT id, full_name FROM owners ORDER BY full_name")
    ctx = _liquidacion_plantilla_context(month, owner_id)
    lines = ctx["lines_html"]
    total = ctx["total"]
    admin_eff = ctx["admin_eff"]
    oname = ctx["oname"]
    prop_display = ctx["prop_display"]
    company = ctx["company"]
    email = ctx["email"]
    phone = ctx["phone"]
    ml = ctx["ml"]
    legal = ctx["legal"]
    if owner_id:
        dl_url = url_for("download_liquidacion", month=month, owner_id=owner_id)
    else:
        dl_url = url_for("download_liquidacion", month=month)
    opts = "".join(
        f"<option value='{o['id']}' {'selected' if owner_id == o['id'] else ''}>{o['full_name']}</option>"
        for o in owners_opts
    )
    return f"""
    <!doctype html><html lang="es"><head><meta charset="utf-8"/><base target="_self"/><title>Liquidacion {month}</title>{PRINT_DOC_CSS}<style>@page {{ size: A4 portrait; margin: 14mm; }}</style></head><body>
    <div class="no-print" style="max-width:800px;margin:0 auto 16px;">
      <a class="btn btn-secondary btn-sm" href="/settlements?month={month}">Volver</a>
      <button class="btn btn-primary btn-sm" type="button" onclick="window.print()">Imprimir</button>
      <a class="btn btn-success btn-sm" href="{dl_url}">Guardar PDF en la PC</a>
      <span class="small text-muted ms-2 d-block d-md-inline mt-2 mt-md-0">Listo para enviar por WhatsApp o correo. Elige un propietario abajo para un PDF solo de el/ella.</span>
      <form class="d-inline-block ms-0 ms-md-2 mt-2 mt-md-0" method="get" action="/print/liquidacion">
        <input type="hidden" name="month" value="{month}">
        <select name="owner_id" class="form-select form-select-sm d-inline-block w-auto" onchange="this.form.submit()">
          <option value="">Todos los propietarios</option>
          {opts}
        </select>
      </form>
    </div>
    <div class="doc">
      <div class="doc-header">
        <img class="logo-doc" src="/invoice-logo" alt="" onerror="this.style.display='none'"/>
        <div style="flex:1;">
          <h1 class="title-doc">FACTURA / LIQUIDACION</h1>
          <div class="subtitle">{company}</div>
        </div>
        <div style="width:100px;"></div>
      </div>
      <div class="meta">
        <div class="meta-row"><span class="meta-label">Mes</span><span>{ml}</span></div>
        <div class="meta-row"><span class="meta-label">Propiedad</span><span>{prop_display}</span></div>
        <div class="meta-row"><span class="meta-label">Propietario</span><span>{oname}</span></div>
        <div class="meta-row"><span class="meta-label">% adm. efectivo</span><span>{admin_eff:.2f}% (ponderado sobre beneficio del periodo)</span></div>
      </div>
      <table class="data">
        <thead>
          <tr>
            <th>Fecha reserva</th><th>Nombre cliente</th><th>Monto reserva</th><th>Costos bancarios</th>
            <th>Tarifa limpieza</th><th>Gastos generales</th><th>% adm.</th><th>Beneficio propietario</th>
          </tr>
        </thead>
        <tbody>
          {lines or '<tr><td colspan="8" style="text-align:center;">Sin movimientos en este periodo</td></tr>'}
          <tr class="total-row"><td colspan="7" style="text-align:right;">TOTAL PROPIETARIO (USD)</td><td>${total:,.2f}</td></tr>
        </tbody>
      </table>
      <div class="signatures">
        <div class="sig">
          <strong>Firma Propietario</strong>
          <div class="sig-line">Nombre: __________________ &nbsp; Fecha: __________</div>
        </div>
        <div class="sig">
          <strong>Firma Administrador</strong>
          <p class="mb-1">{company}</p>
          <div class="sig-line">Fecha: __________</div>
        </div>
      </div>
      <div class="footer-doc">{legal}<br/>{email} &middot; {phone}</div>
    </div>
    </body></html>
    """


@app.get("/print/factura/<int:invoice_id>")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def print_factura(invoice_id: int) -> Any:
    inv = fetch_rows(
        """
        SELECT i.*, o.full_name AS owner_name, o.tax_id, o.email AS owner_email
        FROM invoices i
        JOIN owners o ON o.id = i.owner_id
        WHERE i.id = ?
        """,
        (invoice_id,),
    )
    if not inv:
        return "Factura no encontrada", 404
    r = inv[0]
    try:
        saved_inv = int(r["saved_for_accounting"] or 0)
    except (KeyError, TypeError):
        saved_inv = 0
    role_pf = session.get("role", "")
    if role_pf == "contadora" and not saved_inv:
        flash("Esta factura aun no esta en el archivo contable.")
        return redirect(url_for("invoices_archived"))
    guardar_bar = ""
    if role_pf in ("administrador", "secretaria"):
        if saved_inv:
            guardar_bar = (
                '<span class="badge bg-success ms-2 align-middle">Ya guardada en archivo contable</span>'
            )
        else:
            guardar_bar = f"""
      <form class="d-inline ms-2" method="post" action="/invoices/{invoice_id}/guardar-archivo"
            onsubmit="return confirm('Guardar esta factura para que la contadora la vea en Archivo contable?');">
        <button class="btn btn-success btn-sm" type="submit">Guardar en archivo contable</button>
      </form>
      <span class="small text-muted ms-2 d-none d-md-inline">(Mismo boton que en menu Facturas)</span>
            """
    month = r["month"]
    ml = month_label_es(month)
    company = get_setting("company_name")
    email = get_setting("company_email")
    phone = get_setting("company_phone")
    back_href = "/invoices/archived" if role_pf == "contadora" else "/invoices"
    start_m, end_m = parse_month(month)
    agg_g = fetch_rows(
        """
        SELECT COALESCE(SUM(r.general_cost), 0) AS total_general
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        WHERE p.owner_id = ? AND r.checkin >= ? AND r.checkin < ?
        """,
        (r["owner_id"], start_m, end_m),
    )
    total_gastos_gral = float(agg_g[0]["total_general"] or 0) if agg_g else 0.0
    agg_cl = fetch_rows(
        """
        SELECT COALESCE(SUM(r.cleaning_fee), 0) AS t
        FROM reservations r
        JOIN properties p ON p.id = r.property_id
        WHERE p.owner_id = ? AND r.checkin >= ? AND r.checkin < ?
        """,
        (r["owner_id"], start_m, end_m),
    )
    total_limpieza = float(agg_cl[0]["t"] or 0) if agg_cl else 0.0
    total_mantenimiento = owner_period_maintenance_total(int(r["owner_id"]), month)
    subtotal_f = float(r["subtotal"] or 0)
    commission_f = float(r["commission"] or 0)
    admin_pct = (100.0 * commission_f / subtotal_f) if subtotal_f > 0 else 0.0
    return f"""
    <!doctype html><html lang="es"><head><meta charset="utf-8"/><base target="_self"/><title>Factura {invoice_id}</title>{PRINT_DOC_CSS}</head><body>
    <div class="no-print" style="max-width:800px;margin:0 auto 16px;">
      <a class="btn btn-secondary btn-sm" href="{back_href}">Volver</a>
      <button class="btn btn-primary btn-sm" onclick="window.print()">Imprimir / PDF</button>
      {guardar_bar}
    </div>
    <div class="doc">
      <div class="doc-header">
        <img class="logo-doc" src="/logo" alt="" onerror="this.style.display='none'"/>
        <div style="flex:1;">
          <h1 class="title-doc">OnstayRd</h1>
          <div class="subtitle">Factura mensual de reservas</div>
        </div>
        <div style="text-align:right;font-size:0.9rem;">
          <div><strong>No.</strong> {invoice_id:05d}</div>
          <div>{ml}</div>
        </div>
      </div>
      <div class="meta">
        <div class="meta-row"><span class="meta-label">Propietario</span><span>{r['owner_name']}</span></div>
        <div class="meta-row"><span class="meta-label">ID fiscal</span><span>{r['tax_id'] or '—'}</span></div>
        <div class="meta-row"><span class="meta-label">Email</span><span>{r['owner_email'] or '—'}</span></div>
      </div>
      <table class="data">
        <thead><tr><th>Concepto</th><th>Monto (USD)</th></tr></thead>
        <tbody>
          <tr><td>Total gastos generales del periodo <span style="font-weight:400;font-size:0.88em">(descontados en cada reserva al calcular el beneficio)</span></td><td>${total_gastos_gral:,.2f}</td></tr>
          <tr><td>Total tarifas limpieza del periodo <span style="font-weight:400;font-size:0.88em">(por reservas del mes, informativo)</span></td><td>${total_limpieza:,.2f}</td></tr>
          <tr><td>Total facturacion mantenimiento del periodo <span style="font-weight:400;font-size:0.88em">(registrado en Fact. mantenimiento)</span></td><td>${total_mantenimiento:,.2f}</td></tr>
          <tr><td>Subtotal beneficio propietario (mes)</td><td>${r['subtotal']:,.2f}</td></tr>
          <tr><td><strong>Porcentaje de administracion</strong> (efectivo del mes)</td><td><strong>{admin_pct:.2f}%</strong></td></tr>
          <tr><td>Comision administracion OnstayRd ({admin_pct:.2f}%)</td><td>${r['commission']:,.2f}</td></tr>
          <tr class="total-row"><td>Neto a pagar / cargar</td><td>${r['net_amount']:,.2f}</td></tr>
        </tbody>
      </table>
      <p class="footer-doc mt-4">{get_setting('legal_note')}</p>
      <div class="signatures">
        <div class="sig"><strong>Recibido conforme (Propietario)</strong><div class="sig-line">Nombre y firma</div></div>
        <div class="sig"><strong>{company}</strong><p class="small mb-0">{email}<br/>{phone}</p><div class="sig-line">Firma y fecha</div></div>
      </div>
    </div>
    </body></html>
    """


@app.get("/settlements")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def settlements() -> str:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    rows = settlement_rows(month)
    total_owner = sum(r["owner_benefit"] for r in rows)
    admin_eff = settlement_admin_effective_pct(rows)
    lines = "".join(
        f"<tr><td>{r['checkin']}</td><td>{r['owner_name']}</td><td>{r['property_name']}</td><td>{r['guest_name']}</td>"
        f"<td>${r['gross_amount']:.2f}</td><td>${r['bank_cost']:.2f}</td><td>${r['cleaning_fee']:.2f}</td>"
        f"<td>${r['general_cost']:.2f}</td><td>{float(r['commission_pct'] or 0):.2f}%</td>"
        f"<td>${r['owner_benefit']:.2f}</td></tr>"
        for r in rows
    )
    return layout(
        "Liquidaciones",
        f"""
        <div class="card-onstay p-4 mb-3">
          <form class="row g-2 align-items-end flex-wrap" method="get">
            <div class="col-auto">
              <label class="form-label">Mes</label>
              <input class="form-control" type="month" name="month" value="{month}">
            </div>
            <div class="col-auto">
              <button class="btn btn-brand">Calcular</button>
            </div>
            <div class="col-auto">
              <a class="btn btn-outline-gold" href="/settlements-export?month={month}">CSV</a>
            </div>
            <div class="col-auto">
              <a class="btn btn-outline-gold" href="/print/liquidacion?month={month}">Vista impresion (plantilla)</a>
            </div>
            <div class="col-auto">
              <a class="btn btn-outline-secondary" href="/print/liquidacion-final?month={month}">Imprimir liquidacion final</a>
            </div>
            <div class="col-auto">
              <a class="btn btn-success" href="/download/liquidacion?month={month}">Guardar PDF (plantilla)</a>
            </div>
            <div class="col-auto">
              <a class="btn btn-outline-primary" href="/maintenance-billing?month={month}">Fact. mantenimiento</a>
            </div>
            <div class="col-auto ms-auto">
              <a class="btn btn-brand" href="/invoices/create?month={month}">Generar facturas del mes</a>
            </div>
          </form>
          <p class="small text-muted mb-0 mt-2">
            <strong>Vista impresion (plantilla):</strong> documento para enviar a propietarios (WhatsApp/correo); incluye boton <strong>Guardar PDF</strong>.
            <strong>Guardar PDF (plantilla):</strong> descarga el mismo PDF sin abrir la vista.
            <strong>Liquidacion final:</strong> resumen interno con mantenimiento (no es la plantilla de envio).
            <strong>Fact. mantenimiento:</strong> registra cargos del mes.
          </p>
        </div>
        <div class="card-onstay p-4">
          <h3 class="font-serif mb-3">Liquidacion mensual ({month})</h3>
          <div class="table-responsive">
            <table class="table table-hover table-sm align-middle mb-0">
              <thead class="table-light">
                <tr>
                  <th>Fecha</th><th>Propietario</th><th>Propiedad</th><th>Cliente</th>
                  <th>Monto</th><th>Bancarios</th><th>Limpieza</th><th>Gastos gral.</th>
                  <th>% adm.</th><th>Beneficio</th>
                </tr>
              </thead>
              <tbody>
                {lines or '<tr><td colspan="10" class="text-muted">Sin datos</td></tr>'}
              </tbody>
            </table>
          </div>
          <h4 class="text-end font-serif mt-3 mb-0">TOTAL PROPIETARIO: <span class="text-success">${total_owner:,.2f}</span> USD</h4>
          <p class="text-end small text-muted mt-2 mb-0">
            <strong>Porcentaje de administracion efectivo del mes (ponderado):</strong> {admin_eff:.2f}%
          </p>
        </div>
        """,
    )


@app.get("/settlements-export")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def settlements_export() -> Response:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    rows = settlement_rows(month)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "fecha_reserva",
            "propietario",
            "propiedad",
            "cliente",
            "monto_reserva",
            "costos_bancarios",
            "tarifa_limpieza",
            "gastos_generales",
            "porcentaje_administracion",
            "beneficio_propietario",
            "moneda",
        ]
    )
    for r in rows:
        writer.writerow(
            [
                r["checkin"],
                r["owner_name"],
                r["property_name"],
                r["guest_name"],
                r["gross_amount"],
                r["bank_cost"],
                r["cleaning_fee"],
                r["general_cost"],
                float(r["commission_pct"] or 0),
                r["owner_benefit"],
                "USD",
            ]
        )
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=liquidacion_{month}.csv"},
    )


@app.route("/maintenance-billing", methods=["GET", "POST"])
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def maintenance_billing() -> Any:
    month = (request.form.get("month") or request.args.get("month") or "").strip()
    if not month:
        month = datetime.now().strftime("%Y-%m")
    if request.method == "POST" and request.form.get("action") == "add":
        oid = int(request.form["owner_id"])
        desc = (request.form.get("description") or "").strip()
        amt_raw = (request.form.get("amount") or "").strip().replace(",", ".")
        try:
            amt = float(amt_raw)
        except ValueError:
            flash("Monto no valido.")
            return redirect(url_for("maintenance_billing", month=month))
        if not desc:
            flash("Describe el concepto de mantenimiento.")
            return redirect(url_for("maintenance_billing", month=month))
        execute(
            """
            INSERT INTO maintenance_billing(owner_id, month, description, amount, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (oid, month, desc, amt, datetime.now().isoformat()),
        )
        flash("Cargo de mantenimiento registrado.")
        return redirect(url_for("maintenance_billing", month=month))
    if request.method == "POST" and request.form.get("action") == "delete":
        did = int(request.form["delete_id"])
        execute("DELETE FROM maintenance_billing WHERE id = ?", (did,))
        flash("Registro eliminado.")
        return redirect(url_for("maintenance_billing", month=month))
    owners_opts = fetch_rows("SELECT id, full_name FROM owners ORDER BY full_name")
    mrows = fetch_rows(
        """
        SELECT m.*, o.full_name AS owner_name
        FROM maintenance_billing m
        JOIN owners o ON o.id = m.owner_id
        WHERE m.month = ?
        ORDER BY o.full_name, m.id
        """,
        (month,),
    )
    total_m = sum(float(r["amount"] or 0) for r in mrows)
    opt_html = "".join(
        f"<option value='{o['id']}'>{html.escape(o['full_name'])}</option>" for o in owners_opts
    )
    table_m = "".join(
        f"<tr><td>{html.escape(r['owner_name'])}</td><td>{html.escape(r['description'])}</td>"
        f"<td>${float(r['amount']):,.2f}</td><td class='small'>{(r['created_at'] or '')[:19]}</td>"
        f"<td><form method='post' class='d-inline' onsubmit=\"return confirm('Eliminar este cargo?');\">"
        f"<input type='hidden' name='month' value='{html.escape(month)}'>"
        f"<input type='hidden' name='action' value='delete'>"
        f"<input type='hidden' name='delete_id' value='{r['id']}'>"
        f"<button type='submit' class='btn btn-sm btn-outline-danger'>Quitar</button></form></td></tr>"
        for r in mrows
    )
    ml = month_label_es(month)
    return layout(
        "Facturacion de mantenimiento",
        f"""
        <div class="card-onstay p-4 mb-3">
          <h2 class="font-serif mb-2">Facturacion de mantenimiento</h2>
          <p class="text-muted small mb-3">
            Registra aqui los cargos de mantenimiento por propietario y mes. Aparecen en la
            <strong>liquidacion final</strong> y en la <strong>factura</strong> del mismo periodo.
          </p>
          <form class="row g-2 align-items-end flex-wrap mb-0" method="get">
            <div class="col-auto">
              <label class="form-label">Mes</label>
              <input class="form-control" type="month" name="month" value="{month}">
            </div>
            <div class="col-auto">
              <button class="btn btn-brand" type="submit">Ver mes</button>
            </div>
            <div class="col-auto">
              <a class="btn btn-outline-gold" href="/settlements?month={month}">Ir a Liquidaciones</a>
            </div>
          </form>
        </div>
        <div class="row g-4">
          <div class="col-lg-5">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-3">Nuevo cargo ({ml})</h3>
              <form method="post">
                <input type="hidden" name="month" value="{month}">
                <input type="hidden" name="action" value="add">
                <label class="form-label fw-semibold">Propietario</label>
                <select class="form-select mb-2" name="owner_id" required>
                  <option value="">Selecciona...</option>
                  {opt_html}
                </select>
                <label class="form-label fw-semibold">Concepto</label>
                <input class="form-control mb-2" name="description" placeholder="Ej. Reparacion AC, pintura area comun" required>
                <label class="form-label fw-semibold">Monto (USD)</label>
                <input class="form-control mb-3" type="number" step="0.01" name="amount" required>
                <button class="btn btn-brand" type="submit">Registrar mantenimiento</button>
              </form>
            </div>
          </div>
          <div class="col-lg-7">
            <div class="card-onstay p-4">
              <h3 class="font-serif mb-2">Registrado en {ml}</h3>
              <p class="small text-muted">Total mes mantenimiento: <strong>${total_m:,.2f}</strong> USD</p>
              <div class="table-responsive">
                <table class="table table-sm table-hover align-middle">
                  <thead class="table-light">
                    <tr><th>Propietario</th><th>Concepto</th><th>Monto</th><th>Fecha reg.</th><th></th></tr>
                  </thead>
                  <tbody>
                    {table_m or '<tr><td colspan="5" class="text-muted">Sin cargos este mes</td></tr>'}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        """,
    )


def _liquidacion_final_context(month: str) -> dict[str, Any]:
    rows = settlement_rows(month)
    mrows = fetch_rows(
        """
        SELECT m.*, o.full_name AS owner_name
        FROM maintenance_billing m
        JOIN owners o ON o.id = m.owner_id
        WHERE m.month = ?
        ORDER BY o.full_name, m.id
        """,
        (month,),
    )
    total_benefit = sum(float(r["owner_benefit"] or 0) for r in rows)
    total_clean = sum(float(r["cleaning_fee"] or 0) for r in rows)
    total_general = sum(float(r["general_cost"] or 0) for r in rows)
    total_maint = sum(float(r["amount"] or 0) for r in mrows)
    admin_eff = settlement_admin_effective_pct(rows)
    by_owner: dict[int, dict[str, Any]] = {}
    for r in rows:
        oid = int(r["owner_id"])
        if oid not in by_owner:
            by_owner[oid] = {
                "name": r["owner_name"],
                "benefit": 0.0,
                "cleaning": 0.0,
                "general": 0.0,
            }
        by_owner[oid]["benefit"] += float(r["owner_benefit"] or 0)
        by_owner[oid]["cleaning"] += float(r["cleaning_fee"] or 0)
        by_owner[oid]["general"] += float(r["general_cost"] or 0)
    maint_by_owner: dict[int, float] = {}
    for mr in mrows:
        oid = int(mr["owner_id"])
        maint_by_owner[oid] = maint_by_owner.get(oid, 0.0) + float(mr["amount"] or 0)
    detail_lines = "".join(
        f"<tr><td>{r['checkin']}</td><td>{html.escape(r['owner_name'])}</td>"
        f"<td>{html.escape(r['property_name'])}</td><td>{html.escape(r['guest_name'])}</td>"
        f"<td>${float(r['gross_amount']):,.2f}</td><td>${float(r['bank_cost']):,.2f}</td>"
        f"<td>${float(r['cleaning_fee']):,.2f}</td><td>${float(r['general_cost']):,.2f}</td>"
        f"<td>{float(r['commission_pct'] or 0):.2f}%</td>"
        f"<td>${float(r['owner_benefit']):,.2f}</td></tr>"
        for r in rows
    )
    maint_lines = "".join(
        f"<tr><td>{html.escape(mr['owner_name'])}</td>"
        f"<td>{html.escape(mr['description'])}</td><td>${float(mr['amount']):,.2f}</td></tr>"
        for mr in mrows
    )
    summary_rows: list[str] = []
    all_oids = sorted(set(by_owner.keys()) | set(maint_by_owner.keys()))
    for oid in all_oids:
        bo = by_owner.get(oid)
        if bo:
            name = bo["name"]
            ben = bo["benefit"]
            cln = bo["cleaning"]
            gen = bo["general"]
        else:
            nrow = fetch_rows("SELECT full_name FROM owners WHERE id = ?", (oid,))
            name = nrow[0]["full_name"] if nrow else "—"
            ben = cln = gen = 0.0
        mnt = maint_by_owner.get(oid, 0.0)
        summary_rows.append(
            f"<tr><td>{html.escape(str(name))}</td><td>${ben:,.2f}</td><td>${cln:,.2f}</td>"
            f"<td>${gen:,.2f}</td><td>${mnt:,.2f}</td></tr>"
        )
    summary_html = "".join(summary_rows)
    return {
        "month": month,
        "total_benefit": total_benefit,
        "total_clean": total_clean,
        "total_general": total_general,
        "total_maint": total_maint,
        "admin_eff": admin_eff,
        "detail_lines": detail_lines,
        "maint_lines": maint_lines,
        "summary_html": summary_html,
        "company": get_setting("company_name"),
        "email": get_setting("company_email"),
        "phone": get_setting("company_phone"),
        "ml": month_label_es(month),
        "legal": get_setting("legal_note"),
    }


def _liquidacion_final_pdf_html(ctx: dict[str, Any]) -> str:
    c = html.escape(str(ctx.get("company") or ""))
    ml = html.escape(str(ctx.get("ml") or ""))
    em = html.escape(str(ctx.get("email") or ""))
    ph = html.escape(str(ctx.get("phone") or ""))
    leg = html.escape(str(ctx.get("legal") or ""))
    det = ctx["detail_lines"] or (
        '<tr><td colspan="10" style="text-align:center;">Sin reservas en el mes</td></tr>'
    )
    maint_body = ctx["maint_lines"] or (
        '<tr><td colspan="3" style="text-align:center;">Sin cargos de mantenimiento registrados</td></tr>'
    )
    summ = ctx["summary_html"] or '<tr><td colspan="5" style="text-align:center;">Sin datos</td></tr>'
    tb, tc, tg, tm = ctx["total_benefit"], ctx["total_clean"], ctx["total_general"], ctx["total_maint"]
    ae = ctx["admin_eff"]
    logo_uri = _logo_data_uri_for_pdf()
    logo_block = (
        f'<img class="logo-img" src="{logo_uri}" alt=""/>'
        if logo_uri
        else '<div class="logo-img">&nbsp;</div>'
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"/>
<style>
  @page {{ size: a4 landscape; margin: 10mm; }}
  body {{
    margin: 0; padding: 0; background: #faf7f2; font-family: Helvetica, Arial, sans-serif;
    color: #2c2618; font-size: 8.5pt;
  }}
  .doc {{
    background: #fff; border: 1px solid #e6dfcf; padding: 20px 24px; margin: 0 auto;
  }}
  .hdr-sep {{ border-bottom: 2px solid #9e7f44; margin: 12px 0 16px 0; padding-top: 4px; }}
  .title-doc {{
    font-family: Georgia, 'Times New Roman', serif; font-size: 18pt; font-weight: 600;
    letter-spacing: 0.08em; text-align: center; margin: 0; color: #2c2618; line-height: 1.1;
  }}
  .subtitle {{ text-align: center; font-size: 10pt; color: #6b6356; margin-top: 6px; }}
  .periodo-box {{ text-align: right; font-size: 9.5pt; vertical-align: top; padding-top: 4px; }}
  .meta-table {{ width: 100%; margin: 12px 0; font-size: 9pt; border-collapse: collapse; }}
  .meta-table td {{ border-bottom: 1px solid #e6dfcf; padding: 6px 6px; vertical-align: top; }}
  .meta-label {{ width: 220px; font-weight: bold; color: #9e7f44; }}
  .section-title {{
    font-family: Georgia, 'Times New Roman', serif; font-size: 11pt; color: #9e7f44;
    margin: 16px 0 6px 0; font-weight: 600;
  }}
  table.data {{ width: 100%; border-collapse: collapse; font-size: 7.5pt; margin-top: 6px; }}
  table.data th {{
    background: #f2e9de; color: #4a402e; padding: 6px 5px; text-align: left;
    border: 1px solid #e6dfcf; font-weight: bold;
  }}
  table.data td {{ padding: 5px; border: 1px solid #e6dfcf; vertical-align: top; }}
  .total-row td {{ background: #f2e9de; font-weight: bold; }}
  .footer-doc {{ margin-top: 16px; font-size: 7.5pt; color: #6b6356; text-align: center; }}
  .logo-cell {{ width: 100px; vertical-align: top; padding-right: 10px; }}
  .logo-img {{ width: 92px; height: 92px; border-radius: 46px; background: #f2e9de; padding: 6px; }}
</style></head><body>
<div class="doc">
  <table width="100%" cellpadding="0" cellspacing="0" border="0">
    <tr>
      <td class="logo-cell">{logo_block}</td>
      <td align="center" valign="middle">
        <div class="title-doc">LIQUIDACION FINAL DEL MES</div>
        <div class="subtitle">{c}</div>
      </td>
      <td class="periodo-box" width="120"><strong>Periodo</strong><br/>{ml}</td>
    </tr>
  </table>
  <div class="hdr-sep"></div>
  <table class="meta-table">
    <tr><td class="meta-label">Total beneficio propietarios</td><td>${tb:,.2f} USD</td></tr>
    <tr><td class="meta-label">Total tarifas limpieza (reservas)</td><td>${tc:,.2f} USD</td></tr>
    <tr><td class="meta-label">Total gastos generales (reservas)</td><td>${tg:,.2f} USD</td></tr>
    <tr><td class="meta-label">Total mantenimiento facturado</td><td>${tm:,.2f} USD</td></tr>
    <tr><td class="meta-label">% adm. efectivo (ponderado)</td><td>{ae:.2f}%</td></tr>
  </table>
  <div class="section-title">Detalle por reserva</div>
  <table class="data">
    <thead><tr>
      <th>Fecha</th><th>Propietario</th><th>Propiedad</th><th>Cliente</th>
      <th>Monto</th><th>Bancarios</th><th>Limpieza</th><th>Gastos gral.</th><th>% adm.</th><th>Beneficio</th>
    </tr></thead>
    <tbody>{det}</tbody>
  </table>
  <div class="section-title">Facturacion de mantenimiento del mes</div>
  <table class="data">
    <thead><tr><th>Propietario</th><th>Concepto</th><th>Monto (USD)</th></tr></thead>
    <tbody>{maint_body}
    <tr class="total-row"><td colspan="2" style="text-align:right;">TOTAL MANTENIMIENTO</td><td>${tm:,.2f}</td></tr>
    </tbody>
  </table>
  <div class="section-title">Resumen por propietario</div>
  <table class="data">
    <thead><tr>
      <th>Propietario</th><th>Beneficio (mes)</th><th>Limpiezas</th><th>Gastos gral.</th><th>Mantenimiento</th>
    </tr></thead>
    <tbody>{summ}</tbody>
  </table>
  <div class="footer-doc">{leg}<br/>{em} &middot; {ph}</div>
</div>
</body></html>"""


def _liquidacion_final_pdf_bytes(ctx: dict[str, Any]) -> bytes:
    from io import StringIO

    from xhtml2pdf import pisa

    out = io.BytesIO()
    html_doc = _liquidacion_final_pdf_html(ctx)
    result = pisa.CreatePDF(StringIO(html_doc), dest=out, encoding="utf-8")
    if result.err:
        raise RuntimeError("Fallo al generar PDF")
    return out.getvalue()


@app.get("/download/liquidacion-final")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def download_liquidacion_final() -> Any:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    ctx = _liquidacion_final_context(month)
    try:
        pdf_data = _liquidacion_final_pdf_bytes(ctx)
    except Exception as e:
        traceback.print_exc()
        flash(
            f"No se pudo generar el PDF: {str(e) or 'error desconocido'}. "
            "Revisa la terminal donde corre la app para ver el detalle."
        )
        return redirect(url_for("settlements", month=month))
    fname = f"liquidacion_final_{month}.pdf"
    return send_file(
        io.BytesIO(pdf_data),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname,
    )


@app.get("/print/liquidacion-final")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def print_liquidacion_final() -> str:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    ctx = _liquidacion_final_context(month)
    total_benefit = ctx["total_benefit"]
    total_clean = ctx["total_clean"]
    total_general = ctx["total_general"]
    total_maint = ctx["total_maint"]
    admin_eff = ctx["admin_eff"]
    detail_lines = ctx["detail_lines"]
    maint_lines = ctx["maint_lines"]
    summary_html = ctx["summary_html"]
    company = ctx["company"]
    email = ctx["email"]
    phone = ctx["phone"]
    ml = ctx["ml"]
    legal = ctx["legal"]
    dl_plantilla = url_for("download_liquidacion", month=month)
    dl_final = url_for("download_liquidacion_final", month=month)
    return f"""
    <!doctype html><html lang="es"><head><meta charset="utf-8"/><base target="_self"/><title>Liquidacion final {month}</title>
    {PRINT_DOC_CSS}</head><body>
    <div class="no-print" style="max-width:900px;margin:0 auto 16px;">
      <a class="btn btn-secondary btn-sm" href="/settlements?month={month}">Volver</a>
      <button class="btn btn-primary btn-sm" type="button" onclick="window.print()">Imprimir</button>
      <a class="btn btn-success btn-sm" href="{dl_final}">Guardar PDF (esta vista)</a>
      <a class="btn btn-outline-success btn-sm" href="{dl_plantilla}">PDF plantilla propietarios</a>
      <span class="small text-muted ms-2 d-block d-md-inline mt-2 mt-md-0">
        <strong>Guardar PDF (esta vista):</strong> igual que ves abajo (logo, totales, mantenimiento).
        <strong>PDF plantilla:</strong> documento corto para enviar a dueños (Vista impresion).
      </span>
    </div>
    <div class="doc" style="max-width:900px;">
      <div class="doc-header">
        <img class="logo-doc" src="/logo" alt="" onerror="this.style.display='none'"/>
        <div style="flex:1;">
          <h1 class="title-doc">LIQUIDACION FINAL DEL MES</h1>
          <div class="subtitle">{company}</div>
        </div>
        <div style="text-align:right;font-size:0.9rem;"><div><strong>Periodo</strong></div><div>{ml}</div></div>
      </div>
      <div class="meta">
        <div class="meta-row"><span class="meta-label">Total beneficio propietarios</span><span>${total_benefit:,.2f} USD</span></div>
        <div class="meta-row"><span class="meta-label">Total tarifas limpieza (reservas)</span><span>${total_clean:,.2f} USD</span></div>
        <div class="meta-row"><span class="meta-label">Total gastos generales (reservas)</span><span>${total_general:,.2f} USD</span></div>
        <div class="meta-row"><span class="meta-label">Total mantenimiento facturado</span><span>${total_maint:,.2f} USD</span></div>
        <div class="meta-row"><span class="meta-label">% adm. efectivo (ponderado)</span><span>{admin_eff:.2f}%</span></div>
      </div>
      <h2 class="h5 font-serif mt-4 mb-2" style="color:#9e7f44;">Detalle por reserva</h2>
      <table class="data">
        <thead><tr>
          <th>Fecha</th><th>Propietario</th><th>Propiedad</th><th>Cliente</th>
          <th>Monto</th><th>Bancarios</th><th>Limpieza</th><th>Gastos gral.</th><th>% adm.</th><th>Beneficio</th>
        </tr></thead>
        <tbody>
          {detail_lines or '<tr><td colspan="10" style="text-align:center;">Sin reservas en el mes</td></tr>'}
        </tbody>
      </table>
      <h2 class="h5 font-serif mt-4 mb-2" style="color:#9e7f44;">Facturacion de mantenimiento del mes</h2>
      <table class="data">
        <thead><tr><th>Propietario</th><th>Concepto</th><th>Monto (USD)</th></tr></thead>
        <tbody>
          {maint_lines or '<tr><td colspan="3" style="text-align:center;">Sin cargos de mantenimiento registrados</td></tr>'}
          <tr class="total-row"><td colspan="2" style="text-align:right;">TOTAL MANTENIMIENTO</td><td>${total_maint:,.2f}</td></tr>
        </tbody>
      </table>
      <h2 class="h5 font-serif mt-4 mb-2" style="color:#9e7f44;">Resumen por propietario</h2>
      <table class="data">
        <thead><tr>
          <th>Propietario</th><th>Beneficio (mes)</th><th>Limpiezas</th><th>Gastos gral.</th><th>Mantenimiento</th>
        </tr></thead>
        <tbody>
          {summary_html or '<tr><td colspan="5" class="text-muted">Sin datos</td></tr>'}
        </tbody>
      </table>
      <div class="footer-doc mt-4">{legal}<br/>{email} &middot; {phone}</div>
    </div>
    </body></html>
    """


@app.get("/invoices/create")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def create_invoices() -> Any:
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    rows = settlement_rows(month)
    by_owner: dict[int, dict[str, Any]] = {}
    for row in rows:
        owner_id = row["owner_id"]
        if owner_id not in by_owner:
            by_owner[owner_id] = {"name": row["owner_name"], "subtotal": 0.0, "commission": 0.0}
        by_owner[owner_id]["subtotal"] += row["owner_benefit"]
        by_owner[owner_id]["commission"] += row["owner_benefit"] * (row["commission_pct"] / 100)
    created = 0
    with db() as conn:
        for owner_id, data in by_owner.items():
            subtotal = data["subtotal"]
            commission = data["commission"]
            net_amount = subtotal - commission
            conn.execute(
                """
                INSERT INTO invoices(
                    owner_id, month, subtotal, commission, net_amount, status, created_at, saved_for_accounting
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?, 0)
                """,
                (owner_id, month, subtotal, commission, net_amount, datetime.now().isoformat()),
            )
            created += 1
    flash(f"Facturas creadas: {created}")
    return redirect(url_for("invoices"))


@app.get("/invoices")
@login_required
@roles_allowed("administrador", "secretaria")
def invoices() -> str:
    rows = fetch_rows(
        """
        SELECT i.*, o.full_name AS owner_name
        FROM invoices i
        JOIN owners o ON o.id = i.owner_id
        ORDER BY i.saved_for_accounting ASC, i.id DESC
        """
    )
    rows_html: list[str] = []
    for r in rows:
        saved = int(r["saved_for_accounting"] or 0)
        badge = (
            "<span class='badge bg-success'>En archivo contable</span>"
            if saved
            else "<span class='badge bg-warning text-dark'>Borrador</span>"
        )
        saved_info = ""
        if saved and r["saved_at"]:
            who = r["saved_by_username"] or "—"
            saved_info = f"<div class='small text-muted'>{r['saved_at'][:19]} por {who}</div>"
        pdf_btn = (
            f"<a class='btn btn-sm btn-outline-gold' href='/print/factura/{r['id']}'>Ver PDF</a>"
        )
        guardar_form = ""
        if not saved:
            guardar_form = f"""
            <form class='d-inline' method='post' action='/invoices/{r['id']}/guardar-archivo'
                  onsubmit="return confirm('Guardar esta factura en el archivo contable? La contadora podra verla.');">
              <button class='btn btn-sm btn-success' type='submit'>Guardar en archivo</button>
            </form>
            """
        _st = float(r["subtotal"] or 0)
        _cm = float(r["commission"] or 0)
        _adm_pct = (100.0 * _cm / _st) if _st > 0 else 0.0
        _cl, _mt = invoice_extra_amounts(int(r["owner_id"]), str(r["month"]))
        rows_html.append(
            f"<tr><td>{r['id']}</td><td>{r['month']}</td><td>{r['owner_name']}</td>"
            f"<td>${_cl:.2f}</td><td>${_mt:.2f}</td>"
            f"<td>${r['subtotal']:.2f}</td><td>{_adm_pct:.2f}%</td><td>${r['commission']:.2f}</td><td>${r['net_amount']:.2f}</td>"
            f"<td>{badge}{saved_info}</td><td>{pdf_btn} {guardar_form}</td></tr>"
        )
    table = "".join(rows_html)
    return layout(
        "Facturas",
        f"""
        <div class="card-onstay p-4 mb-3">
          <h3 class="font-serif mb-2">Todas las facturas generadas</h3>
          <p class="text-muted small mb-2">
            Las facturas nuevas quedan como <strong>Borrador</strong>. En la columna <strong>Acciones</strong> veras el boton verde
            <strong>Guardar en archivo</strong> (solo mientras sea borrador). Tambien esta en <strong>Ver PDF</strong> arriba a la derecha.
            La contadora <strong>no</strong> ve este menu: usa usuario <strong>admin</strong> o <strong>secretaria</strong>.
          </p>
          <p class="text-muted small mb-0">
            Si ya dice <span class="badge bg-success">En archivo contable</span>, el boton Guardar desaparece (ya esta guardada).
            <a href="/invoices/archived">Ir a Archivo contable</a>
          </p>
        </div>
        <div class="card-onstay p-4">
          <div class="table-responsive">
            <table class="table table-hover align-middle mb-0">
              <thead class="table-light">
                <tr>
                  <th>ID</th><th>Mes</th><th>Propietario</th><th>Limpiezas</th><th>Mantenimiento</th>
                  <th>Subtotal</th><th>% Adm.</th><th>Comision</th><th>Neto</th>
                  <th>Archivo</th><th>Acciones</th>
                </tr>
              </thead>
              <tbody>{table or '<tr><td colspan="11" class="text-muted">Sin datos. Genera facturas desde Liquidaciones.</td></tr>'}</tbody>
            </table>
          </div>
        </div>
        """,
    )


@app.post("/invoices/<int:invoice_id>/guardar-archivo")
@login_required
@roles_allowed("administrador", "secretaria")
def guardar_factura_archivo(invoice_id: int) -> Any:
    row = fetch_rows("SELECT id, saved_for_accounting FROM invoices WHERE id = ?", (invoice_id,))
    if not row:
        flash("Factura no encontrada.")
        return redirect(url_for("invoices"))
    if int(row[0]["saved_for_accounting"] or 0):
        flash("Esta factura ya estaba en el archivo.")
        return redirect(url_for("invoices"))
    uname = session.get("username") or "usuario"
    execute(
        """
        UPDATE invoices
        SET saved_for_accounting = 1, saved_at = ?, saved_by_username = ?
        WHERE id = ?
        """,
        (datetime.now().isoformat(), uname, invoice_id),
    )
    flash("Factura guardada en el archivo contable. La contadora ya puede verla.")
    return redirect(url_for("invoices"))


@app.get("/invoices/archived")
@login_required
@roles_allowed("administrador", "contadora", "secretaria")
def invoices_archived() -> str:
    rows = fetch_rows(
        """
        SELECT i.*, o.full_name AS owner_name
        FROM invoices i
        JOIN owners o ON o.id = i.owner_id
        WHERE i.saved_for_accounting = 1
        ORDER BY i.saved_at DESC, i.id DESC
        """
    )
    role = session.get("role", "")
    intro = ""
    if role == "contadora":
        intro = "<p class='text-muted small'>Solo se muestran facturas que administracion haya <strong>guardado en archivo</strong>.</p>"
    else:
        intro = "<p class='text-muted small'>Mismo listado que ve la contadora. <a href='/invoices'>Ver todas incl. borradores</a></p>"
    def _inv_row_arch(r: sqlite3.Row) -> str:
        _st = float(r["subtotal"] or 0)
        _cm = float(r["commission"] or 0)
        _adm_pct = (100.0 * _cm / _st) if _st > 0 else 0.0
        _cl, _mt = invoice_extra_amounts(int(r["owner_id"]), str(r["month"]))
        return (
            f"<tr><td>{r['id']}</td><td>{r['month']}</td><td>{r['owner_name']}</td>"
            f"<td>${_cl:.2f}</td><td>${_mt:.2f}</td>"
            f"<td>${r['subtotal']:.2f}</td><td>{_adm_pct:.2f}%</td><td>${r['commission']:.2f}</td><td>${r['net_amount']:.2f}</td>"
            f"<td class='small'>{(r['saved_at'] or '')[:19]}<br/><span class='text-muted'>{r['saved_by_username'] or ''}</span></td>"
            f"<td><a class='btn btn-sm btn-outline-gold' href='/print/factura/{r['id']}'>Ver PDF</a></td></tr>"
        )

    table = "".join(_inv_row_arch(r) for r in rows)
    return layout(
        "Archivo contable",
        f"""
        <div class="card-onstay p-4">
          <h3 class="font-serif mb-2">Archivo de facturas (contabilidad)</h3>
          {intro}
          <div class="table-responsive">
            <table class="table table-hover align-middle mb-0">
              <thead class="table-light">
                <tr>
                  <th>ID</th><th>Mes</th><th>Propietario</th><th>Limpiezas</th><th>Mantenimiento</th>
                  <th>Subtotal</th><th>% Adm.</th><th>Comision</th><th>Neto</th>
                  <th>Guardada</th><th>PDF</th>
                </tr>
              </thead>
              <tbody>{table or '<tr><td colspan="11" class="text-muted">Aun no hay facturas en archivo.</td></tr>'}</tbody>
            </table>
          </div>
        </div>
        """,
    )


if __name__ == "__main__":
    init_db()
    try:
        from xhtml2pdf import pisa  # noqa: F401
    except ImportError:
        print(
            "\n*** AVISO: xhtml2pdf no esta instalado. Para descargar PDF de liquidaciones, ejecuta:\n"
            "    python -m pip install xhtml2pdf\n"
            "    y reinicia esta app.\n"
        )
    # Acceso desde otras PCs en la misma red: ONSTAY_HOST=0.0.0.0 (ver ACCESS.md)
    _host = os.environ.get("ONSTAY_HOST", "127.0.0.1")
    _port = int(os.environ.get("ONSTAY_PORT", "5000"))
    _debug = os.environ.get("ONSTAY_DEBUG", "1").lower() in ("1", "true", "yes")
    app.run(host=_host, port=_port, debug=_debug)
