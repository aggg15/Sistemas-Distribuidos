# Release 2 · Adaptación del tutorial «Servicios web – API REST con Node.js»

El tutorial de la asignatura (SD 26/27, 14 páginas) construye una API REST con
**Node.js + Express**: un CRUD de `/usuarios` sobre MySQL, probado con Postman,
y una lectura de un fichero JSON. Este documento explica cómo se ha aplicado a
la práctica y por qué se cambió en algunos puntos. El PDF del tutorial no se
versiona en el repositorio; cada miembro del grupo lo tiene en local.

## 1. Qué módulos usan el tutorial

| Módulo | Tecnología | Motivo |
|---|---|---|
| **WM_Registry** | Node.js + Express (HTTPS) | Es exactamente el caso del tutorial: un CRUD REST (alta, consulta, modificación y baja) sobre una tabla. El enunciado pide «API_Rest… GET, PUT, DELETE» (p. 3). |
| **WM_Front** | Node.js + Express | El enunciado lo recomienda (p. 5: «se recomienda, por su sencillez, el uso de Node.js»). |
| API_Central | Python, dentro de WM_Central | Necesita el estado en memoria de CENTRAL (sesiones, riegos en curso, salud de Kafka). En Node habría que inventar otra interfaz interna entre los dos procesos (ARCHITECTURE.md, D6). |
| WM_W, Monitor, Engine, FO | Python | Corren en los PCs del laboratorio; así no necesitan Node.js. Llaman a las APIs REST con `urllib`. |

Registry y Front se ejecutan en la nube (Docker, p. 9 del enunciado), así que
Node.js solo hace falta en el servidor y en los equipos de desarrollo.

## 2. Paso a paso: tutorial → práctica

| Apartado del tutorial | Qué hace el tutorial | Cómo queda en la práctica |
|---|---|---|
| 1.1–1.2 Instalar Node.js y npm | `sudo apt install nodejs npm` | Node.js **22.13 o posterior** (para `node:sqlite`). El `apt` de Ubuntu puede instalar una versión más antigua: es mejor la de [nodejs.org](https://nodejs.org/es/download/). En la nube se usa la imagen `node:24-slim`. |
| 2.1 `npm init` | Crea `package.json` de `rest_sd` | [`WM_Registry/package.json`](../../WM_Registry/package.json) y [`WM_Front/package.json`](../../WM_Front/package.json), con `main`, `keywords` y el script `start` del tutorial, y además `test` y `engines`. |
| 2.2 `npm install express body-parser request mysql` | Dependencias | Solo `express` (versión fija 5.3.0 y `package-lock.json`, para que `npm ci` instale siempre lo mismo). `body-parser` viene incluido en Express como `express.json()`. `request` está retirado desde 2020: Node.js trae `fetch`. `mysql` se sustituye por `node:sqlite` (ver 3). |
| 2.3 `appSD.js` con `GET /` | Servidor en el puerto 3000 | [`WM_Registry/app.js`](../../WM_Registry/app.js) (`GET /` devuelve el nombre del servicio) y [`WM_Registry.js`](../../WM_Registry/WM_Registry.js), que arranca el servidor **HTTPS** en el 8443. El Front escucha en el 8000. |
| 2.4 `node appSD.js` / `npm start` | Arranque | `npm start` en cada carpeta, o `docker compose up -d registry front`. |
| 3 Base de datos MySQL `SD_MYSQL` | Tabla `Usuarios` con un script SQL | La práctica exige **SQLite** en la nube (p. 9) y CENTRAL ya la usa. El Registry abre el mismo fichero `data/wm.db` (tabla `estaciones`). El esquema lo crea `common/base_datos.py` (Python) y el Registry solo comprueba que exista, para que no haya dos definiciones que puedan divergir. |
| 4.1 `mysql.createConnection` con `root`/`root` | Conexión a la BD | `new DatabaseSync(ruta)` de `node:sqlite`, con `busy_timeout`. No hay usuario ni contraseña que publicar: el fichero vive en el volumen Docker `wm-datos`. |
| 4.2 `GET /usuarios` | Listado | `GET /registry/v1/estaciones`: solo datos públicos (nunca el hash de la credencial ni la clave). |
| 4.3 `GET /usuarios/:id` | Un usuario | `GET /registry/v1/estaciones/:ws_id`: exige la credencial de esa estación (`Authorization: Bearer`). |
| 4.4 `POST /usuarios` | Alta | `PUT /registry/v1/estaciones/:ws_id` con `{"ubicacion": ...}`: responde `201` con la **credencial** (p. 3: «WM_Registry devolverá las claves»). Es PUT y no POST porque el ID lo elige la estación, y repetir la petición no crea dos estaciones (`409`). |
| 4.5 `PUT /usuarios/:id` | Modificación | El mismo `PUT` con la credencial cambia la ubicación (`200`). |
| 4.6 `DELETE /usuarios/:id` | Borrado | `DELETE /registry/v1/estaciones/:ws_id` con la credencial: baja **lógica** (`204`). La fila se conserva porque `riegos` la referencia. Se borran la credencial y la clave, y CENTRAL deja de aceptar la estación. |
| — | — | `POST /registry/v1/estaciones/:ws_id/autenticacion`: la opción de «autenticación» que pide el enunciado (p. 3). |
| 4.x Pruebas con Postman | Capturas de Postman | Colección [`WM_Registry/postman/WM_Registry.postman_collection.json`](../../WM_Registry/postman/WM_Registry.postman_collection.json). La petición «Alta» guarda la credencial en una variable. Además, pruebas automáticas con `node --test` (`npm test`) y, desde Python, por HTTPS real (`tests/test_registry.py`). |
| 5 Fichero `usuarios.json` | `GET /usuarios_json` con `fs.readFileSync` | Mismo patrón para el fichero de ciudades de WM_W (#20), que el enunciado permite gestionar «mediante una opción de menú o un archivo» (p. 5). WM_W (en Python) volverá a leer el fichero cuando cambie, sin reiniciar. |

## 3. Cambios respecto al código del tutorial y por qué

El tutorial es un ejemplo didáctico. Copiado tal cual en un servicio público
con credenciales sería inseguro. Estos son los cambios, por si los pregunta el
profesor:

| Código del tutorial | Problema | En la práctica |
|---|---|---|
| ``SELECT * FROM Usuarios WHERE idUsuario = ${id}`` y el `UPDATE` con `'${nombre}'` | **Inyección SQL**: un `id` como `1 OR 1=1` borra o lee toda la tabla | Consultas con parámetros (`prepare("... WHERE id = ?").get(wsId)`) en todo [`bd.js`](../../WM_Registry/bd.js). |
| `if (error) throw error;` dentro del callback | Un error de BD **tira el servidor** entero | Manejador de errores de Express: `400` (JSON roto), `413` (cuerpo grande), `503` (BD ocupada), `500` (resto), siempre en JSON. |
| `response.send('No hay resultados')` con `200` | El cliente no distingue «vacío» de «no existe» | `200 []` para un listado vacío y `404 {"error": ...}` para una estación que no existe. |
| `response.send('Usuario creado')` | Texto plano | `201` con cabecera `Location` y JSON. |
| HTTP en el puerto 3000 | Datos en claro | **HTTPS** obligatorio (TLS 1.2 o superior) con el certificado de `scripts/generar_certificados.py`. El enunciado exige un canal seguro entre Monitor y Registry (p. 3). |
| Sin autenticación | Cualquiera modifica o borra | Credencial de 256 bits por estación. Solo se guarda su hash PBKDF2 (formato compatible con `common/seguridad.py`, verificable por CENTRAL) y se compara en tiempo constante. |
| `express.json()` sin límite | Cuerpos enormes | Límite de 4 KB y validación de ID y ubicación con el mismo criterio que `campo_valido` de Release 1 (sin `#`, separador del protocolo de sockets). |
| Contraseña `root` de MySQL en el código | Secreto en Git | No hay contraseñas en el código. Los certificados van en `certs/` (ignorado por Git) y se montan en el contenedor. |
| `console.log` por petición | — | Se mantiene, sin credenciales. Además, cada alta, baja y fallo queda en la tabla `auditoria` con fecha, IP y acción (p. 4). |

## 4. Front

El Front sigue el mismo esquema que el tutorial: Express sirve la página
estática (`express.static`) y una ruta `GET /api/*` reenvía la consulta a
API_Central con `fetch`, en lugar de `request`. Es de **solo consulta**: es una
página pública (p. 5) y, si reenviara órdenes, cualquiera con la URL podría
bloquear estaciones o revocar claves. Las órdenes se dan desde WM_Central
(p. 4: «una opción en la Central para restaurar claves»).

## 5. Cómo reproducirlo

```bash
python scripts/generar_certificados.py localhost
python WM_Central/WM_Central.py 9999 --http 8080 &        # crea data/wm.db
(cd WM_Registry && npm ci && npm start) &                  # https://localhost:8443
(cd WM_Front && npm ci && npm start) &                     # http://localhost:8000
curl --cacert certs/ca.pem -X PUT https://localhost:8443/registry/v1/estaciones/WS-04 \
     -H "Content-Type: application/json" -d '{"ubicacion":"River Park"}'
```

O con Docker: `docker compose up -d --build central kafka registry front`.
