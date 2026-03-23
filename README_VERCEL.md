# Onstay PMS en Vercel

Este es el **mismo proyecto Flask** que en tu PC, preparado para desplegar en Vercel.

## Importante

- **Base de datos SQLite** en Vercel vive en `/tmp`: se **reinicia** o puede **perderse** entre despliegues. Úsalo para pruebas o demo. Para producción estable usa un **VPS** o **PostgreSQL**.
- Variable de entorno en Vercel: `ONSTAY_SECRET` (clave larga y aleatoria para sesiones).

## Despliegue

1. Sube esta carpeta a un repositorio de GitHub.
2. En Vercel: **New Project** → importa el repo.
3. **Root Directory**: raíz del repo (donde está `vercel.json`).
4. Añade `ONSTAY_SECRET` en **Environment Variables**.
5. Deploy.

## Usuarios por defecto

Ver `README.md` del proyecto (admin / secretaria / contadora).
