# WM_Front

Aplicación web pública con el cuadro de monitorización de CENTRAL (enunciado
de la Release 2, p. 5). Está hecha con **Node.js + Express**, como recomienda el
enunciado y siguiendo el tutorial «Servicios web – API REST con Node.js».

- `GET /` sirve el dashboard (`public/`).
- `GET /api/...` se reenvía tal cual a API_Central: el navegador solo habla con el Front, así que no hay CORS ni certificados que aceptar.
- `GET /salud` devuelve el estado del propio Front.
- **Solo consulta.** `POST`, `PUT` y `DELETE` sobre `/api` responden `405`. Las órdenes (bloquear, activar, restaurar claves) se dan desde WM_Central, porque el Front es público.

Hoy el dashboard usa `GET /api/estado` de la Release 1: estaciones, riegos en
curso, mensajes de CENTRAL y estado de Kafka. Cuando API_Central v1 (#12, #17)
envíe registro, clave y alerta climática, sus columnas aparecerán solas. El
resto (operarios, clima y mensajes de WM_W) es el Issue #23.

## Puesta en marcha

```bash
cd WM_Front
npm ci
npm start -- --api http://IP_CENTRAL:8080     # http://localhost:8000
```

| Opción | Variable de entorno | Por defecto |
|---|---|---|
| `--puerto` | `WM_FRONT_PORT` | `8000` |
| `--host` | `WM_FRONT_HOST` | `0.0.0.0` |
| `--api` | `WM_API_CENTRAL_URL` | `http://localhost:8080` |

Con Docker: `docker compose up -d --build central kafka front` (perfil `release2`).

Pruebas: `npm test`.
