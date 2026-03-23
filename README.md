# OnstayRd Property Management (PMS)

Sistema para administración de alquileres, liquidaciones en **USD**, coordinación de limpieza y documentos con identidad **OnstayRd**.

## Contacto por defecto (editable en **Empresa**)

- **Correo:** onstayrd@gmail.com  
- **Teléfono:** 829-475-5974  
- **WhatsApp (interno):** configuración `18294755974` (código país + número sin espacios)

## 1) Instalación

```bash
cd C:\Users\VICTUS\onstay-pms
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 2) Ejecutar

```bash
python app.py
```

Abrir: <http://127.0.0.1:5000> (te pedirá **inicio de sesión**).

### Otras PCs, celular o internet (como app web)

El panel **ya funciona en el navegador del teléfono** (misma URL). Para entrar **desde cualquier lugar con internet** y dar acceso por **usuario y clave**, hay que **publicar** la app (servidor + dominio + HTTPS). Guía completa: **`COMO_PUBLICAR_EN_INTERNET.md`**.

Para pruebas rápidas sin servidor propio, ver también **`ACCESS.md`** (misma WiFi, ngrok, Cloudflare Tunnel).

## 3) Usuarios y claves

| Usuario    | Rol            | Contraseña inicial |
|-----------|----------------|---------------------|
| `admin`   | administrador  | `ONSTAY2026`        |
| `secretaria` | secretaria  | `Secretaria2026`    |
| `contadora`  | contadora   | `Contadora2026`     |

- **Administrador:** todo (logo, datos de empresa, **Claves acceso**).
- **Secretaría:** reservas, propiedades, limpieza, liquidaciones, **Facturas** (incl. borradores) y puede **guardar en archivo contable**.
- **Contadora:** liquidaciones, export CSV, **Archivo contable** (solo facturas ya guardadas), impresión PDF.

### Cambiar claves (solo admin)

Menú **Claves acceso** (`/settings/passwords`): define la contraseña de **secretaria** y **contadora**, y opcionalmente cambia la de **admin** (pide la clave actual).

## 3b) Archivo contable (facturas para la contadora)

1. En **Liquidaciones** generas las facturas del mes (quedan como **Borrador**).
2. En **Facturas** revisas cada una y pulsas **Guardar en archivo**.
3. La **contadora** entra a **Archivo contable** y solo ve las que guardaste; puede abrir **Ver PDF**.

Si abre el PDF de una factura que no está archivada, el sistema no se lo permite.

## 4) Documentos tipo plantilla (PDF)

1. **Liquidación mensual:** menú **Liquidaciones** → **Vista impresión (plantilla)** → en el navegador **Imprimir** → **Guardar como PDF**.  
   Puedes filtrar por propietario en la misma pantalla.

2. **Factura por propietario:** **Facturas** (admin/secretaria) o **Archivo contable** (contadora) → **Ver PDF** → imprimir o guardar PDF.

Sube el logo en **Logo** (solo admin).

## 5) WhatsApp – recordatorios de limpieza

No se envía solo desde el servidor (eso requiere **WhatsApp Business API** de pago).  
Lo que hace el sistema: botón **WhatsApp** en cada tarea, con mensaje prellenado: dirección de la propiedad, fecha, hora, huésped y contacto OnstayRd.

**Importante:** en cada tarea indica el **WhatsApp del personal de limpieza** (campo dedicado).

## 6) Flujo recomendado

1. Entrar como **admin** → **Empresa** (revisa correo/teléfono) → **Logo**.  
2. Crear propietarios y propiedades (con dirección completa para limpieza).  
3. iCal Airbnb/Booking → **Sync iCal**.  
4. Completar montos y costos en reservas.  
5. **Limpieza:** crear tarea con teléfono y hora → **WhatsApp**.  
6. **Liquidaciones** → imprimir plantilla → **Generar facturas del mes**.

## Notas

- Base de datos: `onstay.db` (SQLite), en la misma carpeta del proyecto.  
- Variable opcional: `ONSTAY_SECRET` para clave de sesión en producción.
