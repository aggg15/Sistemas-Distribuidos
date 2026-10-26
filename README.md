# Sistemas Distribuidos · Water Management

Práctica de Sistemas Distribuidos del curso 2026/27, desarrollada en Python.
La solución final coordinará estaciones de riego mediante sockets TCP, Kafka y
SQLite, con despliegue Docker en Railway y equipos del laboratorio.

**Estado actual:** base TCP CENTRAL–Monitor integrada con protocolo
STX/ETX/LRC, conexión persistente, latidos, tiempos de espera, reconexión y
control de IDs duplicados. CENTRAL guarda estaciones y operarios en SQLite y
recupera las estaciones como desconectadas al reiniciar. Las utilidades Kafka
comprueban la confirmación de entrega. Todavía no hay un ciclo de riego completo.

## Ejecutar la base de comunicaciones

Desde la raíz del repositorio, con Python 3.9 o posterior:

```bash
# Terminal 1
python3 WM_Central.py 9999 --verbose

# Terminal 2
python3 WM_WS_M.py localhost 9999 WS-04 "River Park"

# Terminal 3, otra estación
python3 WM_WS_M.py localhost 9999 WS-05 "Jardín Botánico"
```

El monitor muestra el registro confirmado y un latido confirmado cada dos
segundos. Ambos programas permanecen en ejecución. Ctrl+C permite detenerlos.
Si se reinicia CENTRAL, el Monitor reintenta y se registra de nuevo. Un segundo
Monitor con el mismo ID recibe un rechazo y termina con código 1.

CENTRAL, el Monitor y SQLite utilizan la biblioteca estándar. Para los ejemplos de Kafka,
instalar la dependencia de `requirements.txt` en un entorno virtual.

## Persistencia SQLite

La base se crea por defecto en `data/wm.db`. Puede elegirse otra ruta mediante
`--db` o `WM_DB_PATH`. Para cargar operarios y consultar los datos:

```bash
.venv/bin/python scripts/gestionar_bd.py operario FO-01 "Operario 1"
.venv/bin/python scripts/gestionar_bd.py operarios
.venv/bin/python scripts/gestionar_bd.py estaciones
```

Al reiniciar CENTRAL con la misma base, los datos permanecen. Las conexiones y
la salud se conocen de nuevo al conectar los módulos. La tabla de riegos está
preparada; todavía no se generan registros de riego. Las consultas muestran
las fechas en horario de Madrid y conservan UTC en la base de datos.

La [guía de persistencia](docs/PERSISTENCIA.md) describe el esquema, las pruebas
de reinicio y la administración de la base local y del volumen Docker.

## Ejecutar con Docker

```bash
docker compose --profile demo up --build central monitor
```

El monitor del contenedor conecta al servicio `central`. Un monitor ejecutado en
el anfitrión puede conectar a `localhost:9999`. Para detener los contenedores:

```bash
docker compose --profile demo down
```

CENTRAL guarda SQLite en un volumen montado en `/app/data`. `down` conserva
los datos; añadir `--volumes` los elimina.

Kafka está configurado como un servicio independiente. Su ejemplo de ejecución
está en [la guía de comunicaciones](docs/COMUNICACIONES.md). El Dockerfile actual
contiene CENTRAL, Monitor y el script de administración SQLite. La integración
de las aplicaciones con Kafka se añadirá al construir el ciclo de riego.

## Estructura y documentación

| Ruta | Contenido |
| --- | --- |
| `WM_Central.py` | Servidor TCP concurrente, sesiones y recuperación de estaciones SQLite. |
| `WM_WS_M.py` | Monitor persistente: registro, PING y reconexión. |
| `common/protocolo.py` | Protocolo STX/ETX/LRC, controles, reintentos y plazos. |
| `common/configuracion.py` | Validación de argumentos y campos. |
| `common/base_datos.py` | Persistencia de estaciones, operarios y esquema del historial. |
| `common/fechas.py` | Conversión de fechas a Europe/Madrid para su presentación. |
| `common/kafka_utils.py` | Productor y consumidor JSON, confirmación y errores de entrega. |
| `scripts/gestionar_bd.py` | Alta de operarios y consulta de los datos persistentes. |
| `scripts/prueba_kafka.py` | Ejemplo independiente de Kafka. |
| `tests/` | Pruebas del protocolo y de las sesiones CENTRAL–Monitor. |
| `anexo-sockets/` | Ejercicio original de registro, con cabecera de longitud. |
| `Dockerfile` / `docker-compose.yml` | Contenedores de la base TCP y broker Kafka local. |
| `docs/COMUNICACIONES.md` | Protocolo actual, argumentos, pruebas y preparación para Railway. |
| `docs/PERSISTENCIA.md` | SQLite, administración, recuperación y volumen Docker. |
| `docs/DISENO.md` | Diseño objetivo de la práctica completa. |
| `docs/USO_IA.md` | Alcance de la asistencia de IA y verificaciones realizadas. |

El anexo se conserva como referencia del aprendizaje inicial. Sus programas usan
un protocolo distinto y deben ejecutarse juntos desde `anexo-sockets`; la
práctica continúa con los programas de la raíz, que utilizan `common`.

## Pruebas

```bash
.venv/bin/python -m unittest -v
docker compose --profile demo config --quiet
```

Las 56 pruebas verifican transporte, sesiones, persistencia, reinicios, IDs
duplicados, concurrencia y confirmación o error de envío Kafka. Las dependencias
de `requirements.txt` deben estar instaladas para ejecutarlas todas.
También se ha construido la imagen y comprobado la persistencia al recrear
CENTRAL en un proyecto Docker de prueba separado.

Los [casos de prueba de SQLite y Kafka](docs/PRUEBAS_SQLITE_KAFKA.md) incluyen
comandos, resultados esperados y pruebas con datos independientes.

## Funcionalidades pendientes

- Escritura del historial y validación de operarios al implementar los riegos.
- `WM_WS_E`, supervisión cada segundo y simulación de fugas.
- `WM_FO`, peticiones manuales y desde fichero con espera de cuatro segundos.
- Integración Kafka y ciclo completo: autorización, riego, telemetría y resumen.
- Panel con los cinco estados, colores y controles individuales y globales.
- Docker para los módulos restantes, persistencia de Kafka y despliegue distribuido.
- Memoria, capturas y guía final de entrega.

Una conexión activa del Monitor no acredita que la estación esté preparada para
regar: esa disponibilidad dependerá también del Engine y de los estados de
negocio que se incorporarán en las siguientes fases. El alta de estaciones es
automática; la validación de solicitudes de operarios contra SQLite se conectará
al implementar FO y el ciclo de riego.

## Railway

Se prevé un servicio para CENTRAL, otro para Kafka y un volumen para SQLite
asociado a CENTRAL. Los monitores y operarios del laboratorio necesitarán los
endpoints públicos adecuados. CENTRAL admite host y puerto configurables y
Railway puede exponer su enlace mediante TCP Proxy.

La guía de comunicaciones explica la diferencia entre puerto interno y público,
la configuración prevista de Kafka y los datos necesarios al desplegar. Esta
versión no acredita un despliegue realizado en Railway.

## Uso de IA

Codex ha ayudado a interpretar el enunciado, diseñar, implementar, documentar y
probar esta base. El alcance de la asistencia y sus verificaciones se describen en
[USO_IA.md](docs/USO_IA.md). El estudiante debe comprender, explicar y poder
modificar el código durante la defensa.
