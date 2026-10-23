# Sistemas Distribuidos · Water Management

Práctica de Sistemas Distribuidos del curso 2026/27, desarrollada en Python.
La solución final coordinará estaciones de riego mediante sockets TCP, Kafka y
SQLite, con despliegue Docker en Railway y equipos del laboratorio.

**Estado actual:** base TCP CENTRAL–Monitor integrada con protocolo
STX/ETX/LRC, conexión persistente, latidos, tiempos de espera, reconexión y
control de IDs duplicados. Todavía no hay un ciclo de riego completo.

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

Esta fase utiliza únicamente la biblioteca estándar. Para los ejemplos de Kafka,
instalar la dependencia de `requirements.txt` en un entorno virtual.

## Ejecutar con Docker

```bash
docker compose --profile demo up --build central monitor
```

El monitor del contenedor conecta al servicio `central`. Un monitor ejecutado en
el anfitrión puede conectar a `localhost:9999`. Para detener los contenedores:

```bash
docker compose --profile demo down
```

Kafka está configurado como un servicio independiente. Su ejemplo de ejecución
está en [la guía de comunicaciones](docs/COMUNICACIONES.md). El Dockerfile actual
contiene la base TCP; la integración de las aplicaciones con Kafka se añadirá
en la siguiente fase.

## Estructura y documentación

| Ruta | Contenido |
| --- | --- |
| `WM_Central.py` | Servidor TCP concurrente y registro de sesiones en memoria. |
| `WM_WS_M.py` | Monitor persistente: registro, PING y reconexión. |
| `common/protocolo.py` | Protocolo STX/ETX/LRC, controles, reintentos y plazos. |
| `common/configuracion.py` | Validación de argumentos y campos. |
| `common/kafka_utils.py` | Utilidades preliminares de productor y consumidor JSON. |
| `scripts/prueba_kafka.py` | Ejemplo independiente de Kafka. |
| `tests/` | Pruebas del protocolo y de las sesiones CENTRAL–Monitor. |
| `anexo-sockets/` | Ejercicio original de registro, con cabecera de longitud. |
| `Dockerfile` / `docker-compose.yml` | Contenedores de la base TCP y broker Kafka local. |
| `docs/COMUNICACIONES.md` | Protocolo actual, argumentos, pruebas y preparación para Railway. |
| `docs/DISENO.md` | Diseño objetivo de la práctica completa. |
| `docs/USO_IA.md` | Alcance de la asistencia de IA y verificaciones realizadas. |

El anexo se conserva como referencia del aprendizaje inicial. Sus programas usan
un protocolo distinto y deben ejecutarse juntos desde `anexo-sockets`; la
práctica continúa con los programas de la raíz, que utilizan `common`.

## Pruebas

```bash
python3 -m unittest -v
docker compose --profile demo config --quiet
```

Las pruebas verifican transporte, sesiones persistentes, registros rechazados,
IDs duplicados, latidos, reconexión, tramas corruptas y fragmentadas, tiempos de
espera y cierres. La validación de Compose comprueba la configuración; construir
las imágenes y comprobar los contenedores requiere Docker en ejecución.

## Funcionalidades pendientes

- SQLite para estaciones, operarios e historial y recuperación al reiniciar.
- `WM_WS_E`, supervisión cada segundo y simulación de fugas.
- `WM_FO`, peticiones manuales y desde fichero con espera de cuatro segundos.
- Integración Kafka y ciclo completo: autorización, riego, telemetría y resumen.
- Panel con los cinco estados, colores y controles individuales y globales.
- Docker para los módulos restantes, persistencia y despliegue distribuido.
- Memoria, capturas y guía final de entrega.

Una conexión activa del Monitor no acredita que la estación esté preparada para
regar: esa disponibilidad dependerá también del Engine y de los estados de
negocio que se incorporarán en las siguientes fases. El registro actual no
sustituye la futura autenticación contra SQLite.

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
