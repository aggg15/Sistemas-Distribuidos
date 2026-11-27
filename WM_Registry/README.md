# WM_Registry

Alta y baja de las estaciones de riego (WS) por API REST sobre HTTPS. En la
respuesta del alta, el Registry entrega la credencial que el Monitor usará para
autenticarse en WM_Central (enunciado de la Release 2, p. 3).

Está hecho con **Node.js + Express**, siguiendo el tutorial «Servicios web – API
REST con Node.js». En [docs/release2/TUTORIAL_API_REST.md](../docs/release2/TUTORIAL_API_REST.md)
se explica cómo se ha adaptado cada paso del tutorial a la práctica.

## API (prefijo `/registry/v1`)

| Verbo | Ruta | Credencial | Respuesta |
|---|---|---|---|
| GET | `/estaciones` | No | `200` lista de estaciones (ID, ubicación, estado de registro, fechas) |
| GET | `/estaciones/{ws_id}` | Sí | `200` datos de la estación · `401` · `404` |
| PUT | `/estaciones/{ws_id}` | No (alta) / Sí (cambio de ubicación) | `201 {…, "credencial"}` alta · `200` ubicación cambiada · `409` ya dada de alta · `401` |
| DELETE | `/estaciones/{ws_id}` | Sí | `204` baja lógica · `401` · `404` |
| POST | `/estaciones/{ws_id}/autenticacion` | Sí | `200 {"ws_id", "autenticada": true}` · `401` · `404` |

- Cuerpo del PUT: `{"ubicacion": "River Park"}`.
- La credencial se envía en la cabecera `Authorization: Bearer <credencial>`.
- Errores: `{"error": "texto"}` con `400`, `401`, `404`, `409`, `413`, `503` (BD ocupada) o `500`.
- Los IDs se pasan a mayúsculas. Ni el ID ni la ubicación pueden llevar `#`, porque es el separador del protocolo de sockets.
- La credencial solo aparece en claro en la respuesta del alta. En la BD se guarda su hash PBKDF2, en el mismo formato que `common/seguridad.py`, para que CENTRAL pueda verificarla.
- Cada alta, baja, cambio y fallo de autenticación queda en la tabla `auditoria` con la IP de origen (`python scripts/gestionar_bd.py auditoria`).

## Puesta en marcha

Hace falta Node.js 22.13 o posterior (incluye `node:sqlite`) y la BD creada por
WM_Central: el Registry espera a que exista con el esquema de la Release 2.

```bash
python scripts/generar_certificados.py localhost   # o la IP pública del servidor
cd WM_Registry
npm ci
npm start                                           # https://localhost:8443/registry/v1
```

| Opción | Variable de entorno | Por defecto |
|---|---|---|
| `--puerto` | `WM_REGISTRY_PORT` | `8443` |
| `--host` | `WM_REGISTRY_HOST` | `0.0.0.0` |
| `--db` | `WM_DB_PATH` | `../data/wm.db` |
| `--cert` | `WM_TLS_CERT` | `../certs/servidor.pem` |
| `--key` | `WM_TLS_KEY` | `../certs/servidor.key` |

Con Docker: `docker compose up -d --build central kafka registry` (perfil `release2`, ver `docker-compose.yml`).

## Probarlo

- Automático: `npm test`, que ejecuta las pruebas con `node --test` sobre una BD creada con `common/base_datos.py`. Además, `python -m unittest tests.test_registry` lo prueba por HTTPS real.
- Postman: importa [postman/WM_Registry.postman_collection.json](postman/WM_Registry.postman_collection.json) y añade `certs/ca.pem` en *Settings → Certificates → CA Certificates*.
- curl:

  ```bash
  curl --cacert certs/ca.pem -X PUT https://localhost:8443/registry/v1/estaciones/WS-04 \
       -H "Content-Type: application/json" -d '{"ubicacion":"River Park"}'
  ```
