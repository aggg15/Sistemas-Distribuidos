# Casos de prueba de la actualización SQLite y Kafka

Ejecutar desde la raíz del repositorio, con el entorno `.venv` preparado y
Docker Desktop en marcha para los casos Docker/Kafka. Cada caso indica su
resultado esperado; este documento sirve para registrar una ejecución manual,
no da por superados los casos por el hecho de enumerarlos.

## Preparación

- Reservar tres terminales: A para CENTRAL, B para el Monitor y C para consultas.
- Usar el puerto **10099** y el fichero **`data/pruebas-actualizacion/wm.db`**.
  Esta base está separada de `data/wm.db` y de la base del contenedor habitual.
- La primera ejecución debe usar una base nueva. Si ya existe esa carpeta,
  elegir otro nombre y sustituirlo en todos los comandos; no es necesario borrar datos.
- En las pruebas Docker se utiliza otro proyecto, `wm-actualizacion-pruebas`, y
  el puerto publicado **10098**. Si esos puertos están ocupados, elegir otros.
- Ejecutar `echo $?` inmediatamente después del comando cuyo código se comprueba.

## P01. Pruebas automatizadas

```bash
.venv/bin/python -m unittest -v
docker compose --profile demo config --quiet
```

**Esperado:** `Ran 56 tests` y `OK`, sin pruebas omitidas. La validación de
Compose termina sin salida y con código 0. Los avisos de cierres y timeout de
las pruebas TCP son intencionados.

Para ejecutar únicamente los 19 casos de persistencia y envío Kafka:

```bash
.venv/bin/python -m unittest -v tests.test_base_datos tests.test_kafka_utils
```

## P02. Crear una base vacía

En C:

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db estaciones
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operarios
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db riegos
```

**Esperado:** las tres consultas devuelven `[]`. Se crean el directorio, el
fichero y las tablas sin necesidad de arrancar CENTRAL.

## P03. Crear y actualizar un operario

En C:

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operario FO-TEST-01 "Operario de pruebas"
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operarios
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operario FO-TEST-01 "Operario actualizado"
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operarios
```

**Esperado:** inicialmente aparece un operario. Tras actualizarlo sigue habiendo
uno, con ID `FO-TEST-01` y nombre `Operario actualizado`.

## P04. Rechazar datos inválidos

En C:

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operario "" "Nombre"
echo $?
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operario FO-INVALIDO ""
echo $?
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db operarios
```

**Esperado:** ambos comandos de alta muestran error y código **1**. Solo se
conserva el operario válido de P03.

## P05. Registrar una estación y comprobar el guardado

En A:

```bash
.venv/bin/python WM_Central/WM_Central.py 10099 --db data/pruebas-actualizacion/wm.db --verbose
```

En B:

```bash
.venv/bin/python WM_WS/WM_WS_M.py localhost 10099 WS-TEST-01 "Jardín de pruebas"
```

En C:

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db estaciones
```

**Esperado:** B confirma registro y latidos. La consulta muestra una estación,
con ID `WS-TEST-01`, ubicación `Jardín de pruebas`, `bloqueada: 0` y
`ultima_conexion` en horario de Madrid, con desplazamiento explícito (`+02:00`
en verano y `+01:00` en invierno). Es la hora del registro, no la hora a la que
se ejecuta la consulta. La consulta no incluye el estado de conexión: ese dato
pertenece a la memoria de CENTRAL.

Para la comprobación horaria específica:

```bash
.venv/bin/python -m unittest -v tests.test_fechas
```

**Esperado:** cinco pruebas y `OK`, incluidos verano, invierno, cambio de día y
cambio estacional. Consultar conserva los valores UTC originales de SQLite.

## P06. Rechazar un ID activo duplicado sin sobrescribirlo

Dejar A y B funcionando. En C:

```bash
.venv/bin/python WM_WS/WM_WS_M.py localhost 10099 WS-TEST-01 "Ubicación que no debe guardarse"
echo $?
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db estaciones
```

**Esperado:** el segundo Monitor recibe `ID_EN_USO` y termina con código **1**.
El primero sigue confirmando latidos. SQLite mantiene una sola fila y la
ubicación `Jardín de pruebas`.

## P07. Recuperar los datos al reiniciar

1. Detener B con Ctrl+C y después A con Ctrl+C.
2. Volver a ejecutar en A el comando de CENTRAL de P05, con la misma base.
3. Sin arrancar aún el Monitor, consultar `estaciones` y `operarios` en C.

**Esperado:** CENTRAL muestra:

```text
Estación recuperada: WS-TEST-01 | Jardín de pruebas | DESCONECTADA
```

Se conservan la estación y el operario actualizado de P03. La estación no se
presenta como conectada ni disponible por el mero hecho de existir en SQLite.

## P08. Reconectar y actualizar la ubicación sin duplicar

Con A funcionando, arrancar en B:

```bash
.venv/bin/python WM_WS/WM_WS_M.py localhost 10099 WS-TEST-01 "Parque actualizado"
```

Consultar en C:

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db estaciones
```

**Esperado:** registro confirmado, una única fila para `WS-TEST-01`, ubicación
`Parque actualizado` y fecha de última conexión actualizada.

## P09. Mantener los datos tras un cierre abrupto

1. Dejar B funcionando y cerrar el proceso de CENTRAL de A sin permitir su
   parada normal, por ejemplo deteniendo ese proceso desde el monitor del sistema.
2. Volver a ejecutar el comando de CENTRAL de P05.

**Esperado:** se conservan los datos ya confirmados en SQLite. CENTRAL recupera
la estación como desconectada y B se reconecta automáticamente después. La
consulta sigue mostrando una sola fila para su ID. Los riegos reales aún no
existen y no se evalúan en este caso.

## P10. Fallo al abrir la base

La carpeta `data/pruebas-actualizacion` ya existe por P02. Usarla como si fuera
un fichero en C:

```bash
.venv/bin/python WM_Central/WM_Central.py 10100 --db data/pruebas-actualizacion
echo $?
```

**Esperado:** error de base de datos y código **1**. Esa CENTRAL no llega a
escuchar en 10100. La CENTRAL de A y los datos existentes permanecen operativos.

## P11. Integridad y errores de escritura

Estos casos usan bases temporales y no requieren modificar manualmente SQLite:

```bash
.venv/bin/python -m unittest -v tests.test_base_datos.PruebasPersistencia.test_error_al_guardar_no_confirma_ni_crea_una_sesion
.venv/bin/python -m unittest -v tests.test_base_datos.PruebasPersistencia.test_historial_exige_estacion_y_operario_existentes
.venv/bin/python -m unittest -v tests.test_base_datos.PruebasPersistencia.test_reconexion_actualiza_sin_duplicar_ni_perder_bloqueo
.venv/bin/python -m unittest -v tests.test_base_datos.PruebasPersistencia.test_altas_desde_varios_hilos
```

**Esperado:** los cuatro casos terminan en `OK`. Comprueban respectivamente:

- `KO#ERROR_BD` sin sesión ni fila nueva cuando falla el guardado.
- Rechazo de referencias a estaciones u operarios inexistentes en el historial.
- Actualización sin duplicados y conservación de un bloqueo guardado previamente.
- Guardado de 18 estaciones desde seis hilos.

La prueba de bloqueo verifica persistencia del campo; no implica que las órdenes
de bloquear estaciones estén ya implementadas.

## P12. Persistencia al recrear el contenedor Docker

Se puede mantener la prueba local anterior abierta: Docker publicará otro puerto
y usará un volumen independiente. Ejecutar todo este caso desde C:

```bash
WM_SOCKET_PUBLISHED_PORT=10098 WM_WS_ID=WS-DOCKER-01 WM_WS_LOCATION="Parque Docker" \
  docker compose -p wm-actualizacion-pruebas --profile demo up --build -d central monitor

docker compose -p wm-actualizacion-pruebas logs --tail 30 monitor
docker compose -p wm-actualizacion-pruebas exec central python scripts/gestionar_bd.py operario FO-DOCKER-01 "Operario Docker"
docker compose -p wm-actualizacion-pruebas exec central python scripts/gestionar_bd.py estaciones
docker compose -p wm-actualizacion-pruebas exec central python scripts/gestionar_bd.py operarios
```

**Esperado:** registro y latidos de `WS-DOCKER-01`, con ubicación `Parque Docker`.
La base del contenedor contiene también `FO-DOCKER-01`.

Detener el Monitor y recrear CENTRAL conservando el volumen:

```bash
docker compose -p wm-actualizacion-pruebas stop monitor
WM_SOCKET_PUBLISHED_PORT=10098 \
  docker compose -p wm-actualizacion-pruebas up -d --force-recreate central
docker compose -p wm-actualizacion-pruebas logs --tail 30 central
docker compose -p wm-actualizacion-pruebas exec central python scripts/gestionar_bd.py estaciones
docker compose -p wm-actualizacion-pruebas exec central python scripts/gestionar_bd.py operarios
docker compose -p wm-actualizacion-pruebas start monitor
```

**Esperado:** CENTRAL recupera la estación como `DESCONECTADA`. La estación y el
operario siguen guardados y, al arrancar el Monitor, este vuelve a registrarse.
No se duplica la estación.

Para terminar y conservar los datos de la prueba:

```bash
docker compose -p wm-actualizacion-pruebas --profile demo down
```

La base del volumen no es el fichero local `data/pruebas-actualizacion/wm.db`.

## P13. Kafka: mensajes confirmados y recibidos

Cerrar los consumidores anteriores del ejemplo para que un único consumidor
del grupo `prueba` reciba los mensajes. Arrancar el broker habitual:

```bash
docker compose up -d kafka
```

En una terminal, iniciar el consumidor y dejarlo conectado unos segundos:

```bash
.venv/bin/python scripts/prueba_kafka.py consumidor localhost:9092
```

En otra:

```bash
.venv/bin/python scripts/prueba_kafka.py productor localhost:9092
echo $?
```

**Esperado:** cinco mensajes `Enviado y confirmado (offset ...)`, código **0**
y esos cinco mensajes recibidos, numerados del 1 al 5. Los offsets pueden empezar
por un valor distinto de cero porque el topic ya puede contener mensajes.
Repetir el productor debe generar otros cinco mensajes con offsets posteriores.

## P14. Kafka: broker no disponible

Utilizar un puerto local sin broker, por ejemplo 19092, para no detener el Kafka
de uso normal:

```bash
.venv/bin/python scripts/prueba_kafka.py productor localhost:19092
echo $?
```

**Esperado:** error de Kafka y código **1**, sin mensajes `Enviado y confirmado`.
El proceso termina tras el tiempo de conexión; no debe quedar esperando sin límite.
Este caso comprueba un fallo de conexión inicial. Los fallos asíncronos de entrega
y los timeout de confirmación se comprueban por separado en P15.

## P15. Kafka: errores de entrega y confirmación

```bash
.venv/bin/python -m unittest -v tests.test_kafka_utils
```

**Esperado:** siete pruebas y `OK`, sin omisiones. Se comprueban confirmación,
error asíncrono del broker, fallo al encolar, timeout de confirmación, consumo
del plazo al encolar, ausencia de metadatos y timeout de entrada inválido.
Se simulan las respuestas de Kafka: no es necesario cambiar permisos del broker.

## P16. Historial preparado y exclusión de datos de Git

```bash
.venv/bin/python scripts/gestionar_bd.py --db data/pruebas-actualizacion/wm.db riegos
git check-ignore data/pruebas-actualizacion/wm.db
git status --short
```

**Esperado:** historial `[]`, fichero identificado como ignorado y ausencia de
datos SQLite/WAL/SHM en los cambios de Git. Los cambios de código y documentación
de esta actualización sí pueden aparecer. No se generan riegos ficticios para
rellenar la tabla.

## Registro de resultados

Para cada caso anotar fecha, resultado (`OK` o `FALLO`), diferencia respecto a lo
esperado y, si corresponde, captura o salida relevante. Las evidencias más útiles
son P05 (registro y fila), P07 (recuperación desconectada), P08 (actualización sin
duplicados), P12 (datos tras recrear Docker) y P13/P14 (éxito y error de Kafka).
