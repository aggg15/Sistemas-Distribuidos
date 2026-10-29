# Base de comunicaciones de Water Management

Estado: enlace TCP CENTRAL–Monitor implementado, con estaciones persistentes en SQLite. El anexo de sockets original
se conserva en `anexo-sockets/`; los programas que se amplían para la práctica
son `WM_Central/WM_Central.py` y `WM_WS/WM_WS_M.py`.

## 1. Qué se puede ejecutar

Solo se necesita Python 3.9 o posterior para esta fase. Desde la raíz:

```bash
# Terminal 1
python3 WM_Central/WM_Central.py 9999 --verbose

# Terminal 2
python3 WM_WS/WM_WS_M.py localhost 9999 WS-04 "River Park"

# Terminal 3, otra estación
python3 WM_WS/WM_WS_M.py localhost 9999 WS-05 "Jardín Botánico"
```

El monitor confirma el registro y después muestra un latido confirmado cada dos
segundos. Permanece abierto hasta Ctrl+C o SIGTERM. Si CENTRAL no está disponible
al arrancar o la conexión se pierde, reintenta automáticamente. Al reiniciar
CENTRAL, vuelve a saludar y registrarse con una conexión nueva.

Si se ejecuta un segundo monitor con el mismo ID, CENTRAL responde
`KO#ID_EN_USO`. El segundo monitor termina con código **1**. Un monitor previamente
registrado que está reconectando sí reintenta ese rechazo: CENTRAL podría estar
esperando a que expire la sesión anterior tras un corte de red. Los argumentos
locales inválidos producen código **2**; una parada solicitada produce código **0**.

## 2. Protocolo y sesión

```text
Monitor                         CENTRAL
ENQ -------------------------->
    <-------------------------- ACK
STX REGISTRO#ID#ubicacion ETX LRC ->
    <-------------------------- ACK
    <-------------- STX OK#ID ETX LRC
ACK -------------------------->
          [misma conexión]
STX PING#ID ETX LRC ----------->
    <-------------------------- ACK
    <----------------- STX OK ETX LRC
ACK -------------------------->
          [se repite]
EOT --------------------------> cierre
```

- LRC: XOR de los bytes UTF-8 del contenido, sin STX ni ETX.
- Máximo: 4096 bytes de contenido, sin texto vacío ni STX/ETX interiores.
- Se confirma con ACK después de validar checksum, contenido y UTF-8.
- Una trama inválida recibe NACK y puede reenviarse, con tres intentos totales.
- El ACK de transporte no significa que la petición se haya aceptado: una
  petición válida puede recibir una respuesta de aplicación `KO#motivo`.
- Cabeceras inesperadas, cierre durante una trama o tiempo agotado invalidan
  la conexión. No se intenta reutilizar una sesión que puede estar desincronizada.
- Cada operación tiene un plazo total, incluidos sus reintentos. No se renueva
  el plazo por cada byte recibido. `peticion()` y `responder()` también limitan
  el ciclo completo de intercambio.
- Cada socket se utiliza desde un único hilo. No deben iniciarse dos ciclos
  petición/respuesta simultáneos en el mismo `Canal`.

CENTRAL exige un registro inicial. Si lo rechaza, responde KO y cierra la sesión.
Una vez registrada, la sesión admite:

| Petición | Respuesta | Efecto actual |
| --- | --- | --- |
| `PING#<ID>` | `OK` | Mantiene y verifica la conexión. |
| `AVERIA#<ID>` | `OK` | Guarda el aviso de avería en memoria y lo registra en logs. |
| `RESUELTA#<ID>` | `OK` | Retira el aviso y lo registra en logs. |
| Operación desconocida o ID ajeno | `KO#<motivo>` | No modifica otra estación; la sesión sigue abierta. |

ID y ubicación son obligatorios, sin `#`, caracteres de control ni espacios en
los extremos. Se admiten espacios interiores, acentos y otros caracteres Unicode
imprimibles. El ID queda vinculado a su sesión: no puede usarse para actualizar
el estado de otra estación. Un registro repetido no expulsa al monitor original.

La central atiende varias conexiones con hilos, comparte el registro mediante un
lock y limita las conexiones simultáneas. Un cierre abrupto o timeout libera el
ID para permitir una nueva conexión. Al detener CENTRAL se interrumpe la lectura
de sus sockets, se intenta enviar EOT y se espera a que terminen los hilos.

## 3. Tiempos y configuración

Argumentos explícitos tienen prioridad sobre las variables de entorno.

| Programa | Argumento | Variable | Valor por defecto |
| --- | --- | --- | --- |
| CENTRAL | `puerto` | `WM_SOCKET_PORT`, después `PORT` | `9999` |
| CENTRAL | `--host` | `WM_SOCKET_HOST` | `0.0.0.0` |
| CENTRAL | `--timeout` | `WM_SOCKET_TIMEOUT` | `6` s |
| CENTRAL | `--max-conexiones` | `WM_MAX_CONEXIONES` | `64` |
| CENTRAL | `--db` | `WM_DB_PATH` | `data/wm.db` |
| Monitor | `host` | `WM_CENTRAL_HOST` | Obligatorio |
| Monitor | `puerto` | `WM_CENTRAL_PORT` | Obligatorio |
| Monitor | `ws_id` | `WM_WS_ID` | Obligatorio |
| Monitor | `ubicacion` | `WM_WS_LOCATION` | Obligatorio |
| Monitor | `--timeout` | `WM_SOCKET_TIMEOUT` | `3` s |
| Monitor | `--intervalo-ping` | `WM_PING_INTERVAL` | `2` s |
| Monitor | `--reintento` | `WM_RECONNECT_DELAY` | `2` s |

Los tiempos deben ser positivos y finitos. El timeout de CENTRAL debe ser mayor
que el intervalo entre PING y dejar margen para el intercambio. Los valores por
defecto cumplen `6 > 2 + 3`. En una red con más latencia se pueden ampliar
coordinadamente; no conviene cambiar solo el intervalo del monitor.

`WM_SOCKET_TIMEOUT` es una variable por proceso. Compose permite configurarlas
por separado mediante `WM_CENTRAL_TIMEOUT` y `WM_MONITOR_TIMEOUT`.

La firma del Monitor es provisional: el puerto de escucha para Engine y sus
comprobaciones de salud se incorporarán al implementar ese enlace. CENTRAL aún
no recibe argumentos Kafka porque esta versión no lo utiliza.

## 4. Docker local

```bash
docker compose --profile demo up --build central monitor
```

Ejecuta la misma aplicación en dos contenedores. En otra terminal:

```bash
docker compose restart central
docker compose logs --tail 30 central monitor
docker compose --profile demo down
```

El monitor debe volver a registrarse después del reinicio. Para acceder a la
central desde otro equipo, usa la dirección del equipo anfitrión y el puerto
publicado. Dentro de Compose, el monitor conecta a `central` y al puerto interno.
`WM_SOCKET_PUBLISHED_PORT` permite cambiar el puerto del anfitrión sin cambiar
el interno. El nombre de servicio `central` solo se resuelve dentro de Docker.

El Dockerfile empaqueta la base TCP, SQLite y su script de administración.
CENTRAL monta un volumen en `/app/data`; véase [PERSISTENCIA.md](PERSISTENCIA.md).
Kafka conserva su servicio independiente y su ejemplo:

```bash
.venv/bin/python -m pip install -r requirements.txt
docker compose up -d kafka
.venv/bin/python scripts/prueba_kafka.py consumidor localhost:9092
# En otra terminal:
.venv/bin/python scripts/prueba_kafka.py productor localhost:9092
```

El envío y recepción contra el broker local se han comprobado por separado.
Las utilidades esperan la confirmación del mensaje y propagan errores; la
integración con las aplicaciones sigue pendiente del ciclo de riego.

## 5. Railway

Se han consultado las guías oficiales de [TCP Proxy](https://docs.railway.com/networking/tcp-proxy),
[variables](https://docs.railway.com/variables/reference) y
[traducción de Compose a Railway](https://docs.railway.com/guides/docker-compose).

- Railway no ejecuta directamente este Compose: cada componente se configura
  como un servicio independiente del proyecto.
- El servicio de CENTRAL puede construirse con el Dockerfile de la raíz y su
  comando por defecto. Escucha en `0.0.0.0`.
- Configurar explícitamente `WM_SOCKET_PORT=9999` y crear un **TCP Proxy** hacia
  ese puerto interno. Railway asigna un dominio y un puerto público. El Monitor
  del laboratorio necesita esos dos valores, sin `https://` y sin rutas.
- El puerto público asignado no tiene por qué ser 9999. El Monitor no debe usar
  la dirección privada de Railway desde el laboratorio.
- Cuando exista el panel, HTTP y TCP tendrán sus puertos correspondientes;
  `WM_SOCKET_PORT` permitirá separar el TCP de `PORT` usado para HTTP.
- Kafka será otro servicio. Los clientes del laboratorio necesitarán su proxy
  TCP y un listener anunciado con el **dominio y puerto públicos reales**;
  CENTRAL podrá usar un listener interno. No basta con copiar `localhost:9092`
  del Compose a Railway. Esa configuración se cerrará con los endpoints reales.
- SQLite debe usar un volumen del servicio de CENTRAL montado en `/app/data`
  con `WM_DB_PATH=/app/data/wm.db`. La persistencia de Kafka requiere su propio volumen.

No se necesitan credenciales de Railway para desarrollar ni probar esta base.
Para configurar el despliegue harán falta el proyecto/servicios, el dominio y
puerto públicos del proxy de CENTRAL y los datos de conexión de Kafka, además
del montaje elegido para persistencia. Las credenciales se configuran en
Railway, no en el repositorio ni en los ejemplos.

## 6. Pruebas y límites

```bash
.venv/bin/python -m unittest -v
docker compose --profile demo config --quiet
```

Las pruebas automatizadas usan sockets conectados del sistema y los manejadores
reales de CENTRAL y Monitor. Incluyen registro, latidos, duplicados, errores,
fragmentación, checksum, UTF-8, avisos, timeout y reconexión. Capturan también
las excepciones de los hilos para evitar falsos positivos.

La batería completa incluye **56 pruebas** con las dependencias instaladas.
También se han construido y ejecutado los contenedores en un proyecto de prueba,
comprobando persistencia tras recrear CENTRAL y reconexión del Monitor. Se ha
verificado la confirmación y recepción JSON contra Kafka local. El detalle de
estas comprobaciones está en [PERSISTENCIA.md](PERSISTENCIA.md).

Las estaciones y operarios se guardan en SQLite; las sesiones y averías viven en
memoria. No hay todavía Engine, comprobaciones físicas, autorización de riegos,
panel ni consumo de Kafka en CENTRAL. `conectada` describe el enlace con el
Monitor; **no implica DISPONIBLE**. Los avisos AVERIA/RESUELTA están admitidos por
CENTRAL, pero el Monitor aún no los genera automáticamente porque falta Engine.
