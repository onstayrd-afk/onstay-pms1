# OnstayRd en internet: entrar desde cualquier PC o celular

Tu sistema **ya es una web** (navegador). En la oficina solo corre en tu PC; para usarlo **desde casa, otro país o el teléfono** hay que **publicarlo en internet** con una URL y **HTTPS**.

La **clave de usuario** (admin / secretaria / contadora) es lo que controla quién entra: tú das acceso a quien quieras.

---

## Lo que necesitas (resumen)

| Objetivo | Opción típica |
|----------|----------------|
| URL fija, 24/7, teléfono + PC | **Servidor en la nube** o **VPS** + dominio (recomendado) |
| Probar sin pagar servidor todavía | **Cloudflare Tunnel** o **ngrok** desde una PC encendida |
| Máxima privacidad, solo tu equipo | **VPN** (Tailscale) + acceso a tu PC |

---

## Opción A — VPS (recomendada para SQLite)

Un **VPS** es un PC en la nube (DigitalOcean, Linode, Vultr, Hetzner, etc.) donde instalas Python y corres la app con **Gunicorn** + **Nginx** + certificado **Let's Encrypt** (candado verde).

**Ventaja:** el archivo `onstay.db` vive en el disco del servidor y **no se pierde** al reiniciar (a diferencia de algunos planes gratis “efímeros”).

Pasos generales:

1. Crear servidor **Ubuntu** pequeño (1 GB RAM basta al inicio).
2. Instalar Python, clonar o subir la carpeta `onstay-pms`.
3. `pip install -r requirements.txt`
4. Definir variable de entorno **`ONSTAY_SECRET`** (clave larga y aleatoria) para sesiones seguras.
5. Ejecutar con **Gunicorn** (ya está en `requirements.txt` y `Procfile` de ejemplo).
6. Poner **Nginx** delante como proxy y **HTTPS**.
7. Comprar o usar un **dominio** (ej. `pms.onstayrd.com`) apuntando al servidor.

Tu equipo entra con: `https://tu-dominio.com` — mismo login en **Chrome del teléfono** o **Safari**.

---

## Opción B — Render / Railway / Fly.io (PaaS)

Servicios que despliegan desde GitHub con un clic.

**Muy importante:** en el plan **gratis**, el disco suele ser **temporal**. Si usas **SQLite**, la base puede **borrarse** al reiniciar o redeployar.

- Para producción seria ahí conviene **PostgreSQL** (cambio de código: otra base de datos).
- O contratar **disco persistente** en ese proveedor.

Archivos útiles en el proyecto:

- `Procfile` — comando para Gunicorn.
- `runtime.txt` — versión de Python (Render, etc.).

Variables de entorno a configurar en el panel:

- `ONSTAY_SECRET` — obligatorio (cadena larga aleatoria).
- `PORT` — lo suelen poner ellos solos.

---

## Opción C — Sin servidor: túnel desde tu PC

Si **no** quieres VPS todavía, pero necesitas acceso remoto ocasional:

1. Dejas la PC de la oficina encendida con `python app.py` (o Gunicorn).
2. Usas **Cloudflare Tunnel** o **ngrok** (ver `ACCESS.md`).

Obtienes una URL pública **temporal o semi-fija**. Sigue valiendo el **login con clave**.

---

## Seguridad cuando ya está en internet

1. **`ONSTAY_SECRET`** fuerte y única (no la por defecto).
2. Cambiar claves en **Claves acceso** (admin / secretaria / contadora).
3. Solo **HTTPS** (nunca solo HTTP en producción).
4. Copias de **`onstay.db`** periódicas (backup).

---

## Teléfono

La interfaz usa **Bootstrap** y ya tiene `viewport` y ajustes para **toques** (menú hamburguesa, botones más altos). Abre la misma URL en el móvil e inicia sesión como en la PC.

---

## ¿Siguiente paso?

Si me dices qué prefieres (**VPS propio**, **Render**, **solo túnel**), puedo dejarte los **comandos exactos** línea por línea para ese camino (por ejemplo Ubuntu + Nginx + Gunicorn).
