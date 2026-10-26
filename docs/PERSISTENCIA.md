# Persistencia de CENTRAL con SQLite

CENTRAL guarda las estaciones y sus ubicaciones, conserva los operarios
registrados y prepara la tabla del historial de riegos. La base se crea al
arrancar y no necesita un servidor externo.

## Datos y recuperación

| Tabla | Contenido |
| --- | --- |
| `estaciones` | ID, ubicación, bloqueo y fecha de última conexión en UTC. |
| `operarios` | ID y nombre del operario. |
| `riegos` | Estación, operario, origen, inicio, fin, duración, volumen y motivo de finalización. |

Las conexiones TCP y las averías observadas viven en memoria. Al arrancar,
CENTRAL carga las estaciones conocidas como **DESCONECTADA** y lo indica en los
logs. Debe conocer de nuevo su conexión y salud. La conexión del Monitor todavía
no significa que la estación esté disponible para regar: falta incorporar Engine.

Un ID nuevo se da de alta en su primer registro válido. Si ya existe y no tiene
otra sesión activa, se actualizan ubicación y última conexión sin duplicar la
fila ni borrar su bloqueo. Un registro duplicado activo no modifica la base.
CENTRAL envía `OK#<ID>` después de confirmar la transacción SQLite. Si falla la
escritura, responde `KO#ERROR_BD` y no activa la sesión. Los errores al abrir la
base impiden el arranque y producen código 1.

Cada operación abre y cierra una conexión propia, con transacción y claves
foráneas activadas. Se utiliza WAL y un segundo de espera ante bloqueos de
escritura. Las consultas usan parámetros SQL. No se escribe por cada PING.

## Uso local

Desde la raíz del repositorio:

```bash
.venv/bin/python WM_Central.py 9999 --db data/wm.db --verbose
```

La prioridad de configuración es `--db`, variable `WM_DB_PATH` y ruta por defecto
`data/wm.db`. Las rutas relativas dependen del directorio de ejecución. Se crea
el directorio padre si no existe. Los ficheros SQLite y WAL/SHM se excluyen de Git
y del contexto de construcción de Docker.

El script de administración utiliza la misma ruta y puede ejecutarse con
CENTRAL detenida o funcionando:

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/wm.db operario FO-01 "Operario 1"
.venv/bin/python scripts/gestionar_bd.py --db data/wm.db operario FO-02 "Operario 2"
.venv/bin/python scripts/gestionar_bd.py --db data/wm.db operarios
.venv/bin/python scripts/gestionar_bd.py --db data/wm.db estaciones
.venv/bin/python scripts/gestionar_bd.py --db data/wm.db riegos
```

Repetir el alta de un operario actualiza su nombre. Los campos son obligatorios,
imprimibles y sin `#` ni espacios en los extremos. Un error produce código 1.
Las consultas muestran los datos en JSON y convierten `ultima_conexion`,
`inicio` y `fin` a **Europe/Madrid**, con el desplazamiento horario explícito
(`+02:00` en verano, `+01:00` en invierno). SQLite conserva UTC; la conversión
se aplica al presentar los datos, incluidos los ya guardados. Por ejemplo,
`2026-10-23T12:59:21+00:00` se muestra como `2026-10-23T14:59:21+02:00`.
`ultima_conexion` es la hora del registro o reconexión, no el reloj actual ni
la fecha de cada PING. La conexión actual no se guarda en SQLite. El historial estará vacío hasta implementar el ciclo de riego.
`BaseDatos.operario_existe()` prepara la validación de las futuras peticiones FO.

## Prueba de reinicio

1. Arrancar CENTRAL con el comando anterior.
2. En otra terminal, ejecutar:

   ```bash
   .venv/bin/python WM_WS_M.py localhost 9999 WS-04 "River Park"
   ```

3. Consultar `estaciones` con el script y comprobar su ID y ubicación.
4. Detener primero el Monitor y después CENTRAL con Ctrl+C.
5. Arrancar CENTRAL con la misma base. Debe aparecer:

   ```text
   Estación recuperada: WS-04 | River Park | DESCONECTADA
   ```

6. Arrancar el Monitor de nuevo. Debe confirmarse el registro y seguir habiendo
   una sola fila para WS-04. Los operarios también deben conservarse.

Si se deja el Monitor abierto durante el reinicio, se reconecta automáticamente;
se detiene en el paso 4 para observar primero la estación desconectada.

## Docker y Railway

Compose monta `wm-datos` en `/app/data` dentro de CENTRAL y configura
`WM_DB_PATH=/app/data/wm.db`. El volumen conserva los datos al recrear el contenedor:

```bash
docker compose --profile demo up --build -d central monitor
docker compose exec central python scripts/gestionar_bd.py operario FO-01 "Operario 1"
docker compose exec central python scripts/gestionar_bd.py estaciones
docker compose stop monitor
docker compose up -d --force-recreate central
docker compose logs --tail 30 central
docker compose exec central python scripts/gestionar_bd.py estaciones
docker compose exec central python scripts/gestionar_bd.py operarios
docker compose start monitor
```

`docker compose down` conserva el volumen. **`docker compose down --volumes`
elimina también los datos** y no debe usarse para una parada normal.
La base local `data/wm.db` y la del volumen Docker son independientes: para
administrar la segunda, utilizar `docker compose exec central ...`.

En Railway se debe añadir un volumen al servicio de CENTRAL montado en
`/app/data` y mantener `WM_DB_PATH=/app/data/wm.db`. SQLite se ejecuta dentro de
CENTRAL. Véase la guía oficial de [volúmenes de Railway](https://docs.railway.com/volumes).
Esta fase no incluye un despliegue en Railway.

## Confirmación de entrega Kafka

`common.kafka_utils.enviar()` espera la confirmación y devuelve los metadatos
del mensaje. Propaga errores del broker y timeout; `flush()` no se interpreta
como prueba de éxito. El productor limita la espera al encolar a diez segundos,
las peticiones al broker a cinco y la entrega a diez. El plazo de
`enviar(timeout=...)` descuenta la espera de `send()` del tiempo restante para
confirmar; `send()` conserva su propio límite configurado de diez segundos.

Un timeout deja el resultado incierto: el mensaje podría llegar después. La
aplicación no lo reenvía automáticamente. El futuro ciclo de riego deberá
correlacionar las peticiones y evitar ejecutar dos veces la misma orden.
El ejemplo imprime `Enviado y confirmado` tras obtener metadatos y termina con
código 1 si se produce un error. Véase la
[API de KafkaProducer 3.0.11](https://kafka-python.readthedocs.io/en/3.0.11/apidoc/KafkaProducer.html).

## Verificación

```bash
.venv/bin/python -m unittest -v
docker compose --profile demo config --quiet
```

La batería incluye 56 pruebas con `requirements.txt` instalado. Sin
`kafka-python`, las siete pruebas de utilidades Kafka se omiten; para ejecutar
todas se utiliza el entorno virtual del proyecto.

Se verifican persistencia, reinicio como desconectada, conservación del bloqueo,
actualización sin duplicados, altas concurrentes, SQL parametrizado, claves
foráneas y fallos de escritura sin confirmación de registro. Kafka incluye
pruebas de confirmación, errores asíncronos y tiempos de espera. Se comprueba
también la presentación en Madrid, los cambios de día y de horario estacional,
y que consultar no modifica el instante UTC guardado.

Además, se construyó la imagen y se recreó CENTRAL usando un proyecto y volumen
Docker temporales: estación y operario se conservaron y el Monitor volvió a
registrarse sin duplicados. También se confirmaron y recibieron en orden tres
mensajes JSON contra el broker local, usando un topic temporal. Los recursos de
prueba se eliminaron al terminar. No se modificaron los datos de uso normal del
proyecto. Todavía no se ha probado un ciclo completo de riego.

Para repetir las comprobaciones paso a paso, utilizar los
[casos de prueba de SQLite y Kafka](PRUEBAS_SQLITE_KAFKA.md).
