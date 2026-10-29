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
| `common/` | Código compartido: protocolo de sockets, Kafka, SQLite, validaciones. |
| `scripts/gestionar_bd.py` | Alta de operarios y consulta de la base de datos. |
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

# Alta de un operario en la base de datos de CENTRAL
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

## Documentación

- [Diseño](docs/DISENO.md): estados, mensajes, ciclo de riego y resiliencia.
- [Comunicaciones](docs/COMUNICACIONES.md): protocolo de sockets CENTRAL–Monitor.
- [Persistencia](docs/PERSISTENCIA.md): esquema SQLite y administración.
- [Despliegue en AWS](docs/DESPLIEGUE_AWS.md): CENTRAL y Kafka en EC2.
- [Pruebas de SQLite y Kafka](docs/PRUEBAS_SQLITE_KAFKA.md).
