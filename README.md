# Sistemas Distribuidos · Water Management

Práctica de Sistemas Distribuidos del curso 2026/27, en Python. Una red de
estaciones de riego (WS) se coordina desde WM_Central mediante sockets TCP
(protocolo `<STX>DATOS<ETX><LRC>`), Kafka y SQLite. La parte cloud (WM_Central
y Kafka) se despliega en AWS; WM_WS y WM_FO se ejecutan en los equipos del
laboratorio.

## Estructura

| Ruta | Contenido |
| --- | --- |
| `WM_Central/WM_Central.py` | CENTRAL: servidor TCP concurrente de los Monitores, Kafka y SQLite. |
| `WM_Central/gestor_riegos.py` | Ciclo de riego: autorizar, seguir, cortar y cerrar cada riego. |
| `WM_Central/panel_web.py`, `WM_Central/panel/` | Panel web de CENTRAL (estado en tiempo real y órdenes). |
| `WM_WS/WM_WS_M.py` | Monitor de la estación: registro en CENTRAL, latidos y vigilancia del Engine. |
| `WM_WS/WM_WS_E.py` | Engine de la estación: electroválvula, caudalímetro, menú y averías simuladas. |
| `WM_FO/WM_FO.py` | Aplicación del operario: solicitudes manuales o desde fichero. |
| `WM_FO/peticiones_ejemplo.txt` | Fichero de peticiones de ejemplo (`ws_id;segundos`). |
| `WM_Registry/` | Release 2. WM_Registry (Node.js + Express): alta y baja de estaciones por HTTPS. |
| `WM_Front/` | Release 2. Front web (Node.js + Express): dashboard de solo consulta sobre API_Central. |
| `common/` | Código compartido: protocolo de sockets, Kafka, SQLite, seguridad, validaciones. |
| `scripts/gestionar_bd.py` | Alta de estaciones y operarios y consulta de la base de datos. |
| `tests/` | Pruebas automáticas (`python -m unittest`). |
| `deploy/`, `Dockerfile`, `docker-compose.yml` | Contenedores y despliegue en AWS. |
| `docs/` | Diseño, protocolo, persistencia y guía de despliegue. |
| `anexo-sockets/` | Ejercicio inicial de sockets (protocolo distinto, solo referencia). |

## Requisitos

Python 3.9 o posterior y las dependencias de `requirements.txt`:

```bash
pip install -r requirements.txt
```

Los programas se lanzan desde la raíz del repositorio (o desde cualquier
carpeta indicando su ruta). Todos los parámetros (IP, puertos, IDs) se pasan por
argumentos o variables de entorno; `--help` muestra las opciones de cada uno.

## Ejecución

Cada comando en su propia terminal. `KAFKA` es `ip:puerto` del broker (en AWS,
`IP_AWS:9092`) y `CENTRAL` la IP de WM_Central.

```bash
# WM_Central (en AWS se arranca con Docker: docs/DESPLIEGUE_AWS.md)
python WM_Central/WM_Central.py 9999 KAFKA

# Alta de una estación y de un operario en la base de datos de CENTRAL
# (CENTRAL rechaza los que no estén dados de alta)
python scripts/gestionar_bd.py estacion WS-04 "River Park"
python scripts/gestionar_bd.py operario FO1 "Ana"

# Una estación: Monitor y Engine en el mismo equipo
python WM_WS/WM_WS_M.py CENTRAL 9999 WS-04 "River Park" --puerto-engine 9100
python WM_WS/WM_WS_E.py KAFKA localhost 9100

# Un operario
python WM_FO/WM_FO.py KAFKA FO1 --fichero WM_FO/peticiones_ejemplo.txt
```

Para más estaciones en el mismo equipo, cada pareja Monitor/Engine usa otro
`--puerto-engine` (9101, 9102...). El panel de CENTRAL está en
`http://CENTRAL:8080`.

## Pruebas

```bash
python -m unittest -v
```

GitHub Actions las ejecuta con Python 3.9 y 3.12 en cada push a `main` y en
cada pull request; la imagen de Docker Hub solo se publica si pasan.

## Documentación

- [Diseño](docs/DISENO.md): estados, mensajes, ciclo de riego y resiliencia.
- [Comunicaciones](docs/COMUNICACIONES.md): protocolo de sockets CENTRAL–Monitor.
- [Persistencia](docs/PERSISTENCIA.md): esquema SQLite y administración.
- [Despliegue en AWS](docs/DESPLIEGUE_AWS.md): CENTRAL y Kafka en EC2.
- [Pruebas de SQLite y Kafka](docs/PRUEBAS_SQLITE_KAFKA.md).

## Release 2 (en desarrollo)

La Release 2 añade servicios REST y seguridad sobre el sistema anterior:
WM_Registry (alta y baja de estaciones por HTTPS), autenticación de las
estaciones con clave simétrica única y mensajes cifrados, auditoría,
API_Central, Front web y WM_W (clima de OpenWeather cada 4 s). Entrega: semana
del 13/12/2026. Lo descrito arriba corresponde a Release 1 y sigue vigente
mientras no se cierren los Issues de Release 2.

Ya disponibles (Node.js 22.13 o posterior, según el tutorial de API REST de la
asignatura; ver [adaptación del tutorial](docs/release2/TUTORIAL_API_REST.md)):

- [WM_Registry](WM_Registry/README.md): `python scripts/generar_certificados.py localhost` y después `cd WM_Registry && npm ci && npm start` (HTTPS en el 8443).
- [WM_Front](WM_Front/README.md): `cd WM_Front && npm ci && npm start` (HTTP en el 8000).
- Con Docker: `docker compose up -d --build central kafka registry front`.
- Pruebas: `npm test` en cada carpeta. `python -m unittest` incluye la prueba del Registry por HTTPS si se ha hecho `npm ci` en `WM_Registry/`.

Planificación:

- [Matriz de requisitos y trazabilidad](docs/release2/REQUIREMENTS.md)
- [Plan de implementación](docs/release2/IMPLEMENTATION_PLAN.md): orden, ruta crítica, riesgos y checklist de evaluación.
- [Arquitectura y decisiones técnicas](docs/release2/ARCHITECTURE.md): contratos de Registry, sockets, Kafka cifrado y API_Central.
- [Dependencias](docs/release2/DEPENDENCIES.md) y [plan de pruebas](docs/release2/TEST_PLAN.md).
- [Adaptación del tutorial de API REST con Node.js](docs/release2/TUTORIAL_API_REST.md).
- [Uso de IA y prompts](docs/release2/AI_USAGE.md).

Seguimiento en GitHub:
[Issues de Release 2](https://github.com/aggg15/Sistemas-Distribuidos/issues?q=label%3Arelease-2),
[épicas](https://github.com/aggg15/Sistemas-Distribuidos/issues?q=label%3Aepic) y
[milestones](https://github.com/aggg15/Sistemas-Distribuidos/milestones).
