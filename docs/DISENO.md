# Diseño de Water Management (Fase 0)

Este documento recoge las decisiones de diseño tomadas antes de implementar la
práctica completa: estados de las estaciones, protocolo de sockets, eventos de
Kafka, base de datos, panel de CENTRAL y argumentos de cada aplicación.

## Estado de implementación (23 de octubre de 2026)

Este documento describe el **objetivo completo**, no funcionalidades ya
implementadas. La base TCP de CENTRAL y Monitor está integrada en los programas
de la raíz; su contrato actual se describe en [COMUNICACIONES.md](COMUNICACIONES.md).
SQLite ya guarda estaciones y operarios, recupera las estaciones como
desconectadas y prepara la tabla de riegos. Su contrato está en
[PERSISTENCIA.md](PERSISTENCIA.md). Engine, FO, el ciclo de riego y el panel
siguen pendientes.

Actualización (28 de octubre de 2026): `WM_WS/WM_WS_E.py` implementa el Engine (ID y
CHECK con el Monitor, órdenes de Kafka, telemetría y menú) y el Monitor vigila
su salud con `--puerto-engine`, avisando a CENTRAL con `AVERIA` y `RESUELTA`.
El despliegue en la nube pasa a hacerse en **AWS** en lugar de Railway.

Actualización (28 de octubre de 2026, 2): CENTRAL consume Kafka y gestiona el
ciclo de riego completo en `WM_Central/gestor_riegos.py` (solicitudes, autorización,
telemetría, resumen, cortes por avería, desconexión o bloqueo, y órdenes sobre
una estación o todas). `WM_FO/WM_FO.py` implementa la aplicación del operario. El
panel web (apartado 6) muestra el estado y da acceso a las órdenes de CENTRAL.

Actualización (29 de octubre de 2026): resiliencia según la guía de corrección
(apartado 10) y agua total suministrada por estación en el panel.

El registro de comunicaciones actual solo indica si un Monitor está conectado.
El futuro estado DISPONIBLE exigirá además confirmar que el Engine está listo.
Al implementar el ciclo de riego se reservará la estación mientras se tramita
la autorización, evitando dos solicitudes simultáneas para la misma estación.

## 1. Arquitectura

| Comunicación | Tecnología | Uso |
| --- | --- | --- |
| `WM_WS_M` ↔ `WM_Central` | Sockets TCP | Registro, autenticación y estado de salud de la estación. |
| `WM_WS_E` ↔ `WM_WS_M` | Sockets TCP | Asignación del ID y comprobación de salud cada segundo. |
| `WM_Central` ↔ `WM_WS_E` / `WM_FO` | Kafka | Peticiones, órdenes, telemetría y notificaciones. |
| `WM_Central` ↔ BD | SQLite | Solo CENTRAL accede a la base de datos. |

Corresponde con la Figura 3 del enunciado. La línea punteada entre Kafka y el
Monitor se interpreta como opcional y no se utiliza en esta versión.

## 2. Estados de una estación (WS)

CENTRAL no guarda el estado como un único valor. Guarda datos independientes y
**calcula** el estado a partir de ellos. Así no pueden darse combinaciones
inconsistentes, como una estación regando y con fuga al mismo tiempo.

| Dato | Quién lo cambia | ¿Persistente? |
| --- | --- | --- |
| `conectada` | Autenticación del Monitor / pérdida de su conexión | No. Al arrancar todas están desconectadas. |
| `fuga` | Mensajes `AVERIA` y `RESUELTA` del Monitor | No |
| `bloqueada` | Órdenes de bloquear y activar de CENTRAL | Sí, se mantiene tras un reinicio. |
| `riego_actual` | Autorización y fin del riego | Se guarda en el historial de riegos. |

El estado mostrado es el primero que se cumple de la lista:

| Prioridad | Condición | Estado | Color |
| --- | --- | --- | --- |
| 1 | No conectada | DESCONECTADA | Gris |
| 2 | Fuga | FUGA | Rojo |
| 3 | Bloqueada | FUERA DE SERVICIO | Naranja |
| 4 | Riego en curso | REGANDO | Verde parpadeando |
| 5 | Resto | DISPONIBLE | Verde |

Si se detecta una fuga, un bloqueo o una desconexión durante un riego, CENTRAL
lo corta y avisa al operario.

## 3. Protocolo de sockets

Se sigue el flujo recomendado por el enunciado (página 12):

```text
Cliente                          Servidor
  ── connect ───────────────────────▶ accept
  ── <ENQ> ─────────────────────────▶
  ◀───────────────────── <ACK>/<NACK>
  ┌ <STX><REQUEST><ETX><LRC> ───────▶   ┐
  │ ◀────────────────── <ACK>/<NACK>     │ se repite mientras
  │ ◀────── <STX><ANSWER><ETX><LRC>      │ la conexión siga abierta
  └ <ACK>/<NACK> ───────────────────▶   ┘
  ◀────────────── <EOT> ────────────▶
close                              close
```

| Símbolo | Byte |
| --- | --- |
| `STX` | `0x02` |
| `ETX` | `0x03` |
| `EOT` | `0x04` |
| `ENQ` | `0x05` |
| `ACK` | `0x06` |
| `NACK` | `0x15` |

Decisiones:

- **LRC**: XOR byte a byte de REQUEST o ANSWER codificado en UTF-8, sin incluir
  `STX` ni `ETX`, tal como indica `XOR(MESSAGE)` en el enunciado.
- **Delimitación**: se lee hasta `ETX` y después exactamente un byte de LRC. Los
  bytes `0x02` y `0x03` se prohíben explícitamente dentro del contenido, y el LRC puede
  valer cualquier byte porque siempre se lee uno solo.
- **Reintentos**: ante un `NACK` se reenvía la trama hasta 3 veces. Si se agotan,
  la conexión se considera rota.
- **Cierre**: `EOT` indica un cierre ordenado. Si la conexión se corta sin `EOT`,
  se trata como un fallo.
- **Quién inicia cada ciclo**: tras el saludo `ENQ`/`ACK`, cualquiera de los dos
  extremos puede iniciar un ciclo petición/respuesta. Hace falta porque el
  enunciado pide que el Monitor, que actúa como servidor, envíe la comprobación
  de salud al Engine cada segundo.
- **Campos**: separados por `#`. Ningún campo puede contener `#`.

### Mensajes Monitor ↔ CENTRAL

El Monitor es el cliente y mantiene la conexión abierta.

| Petición del Monitor | Respuesta de CENTRAL |
| --- | --- |
| `REGISTRO#<ws_id>#<ubicacion>` | `OK#<ws_id>` o `KO#<motivo>` (ID activo duplicado: `KO#ID_EN_USO`) |
| `AVERIA#<ws_id>` | `OK` |
| `RESUELTA#<ws_id>` | `OK` |
| `PING#<ws_id>` (cada 2 s) | `OK` |

Con los valores por defecto, si CENTRAL agota 6 s esperando un ciclo de un
Monitor, o su conexión se cierra, marca la estación como DESCONECTADA.
El `PING` permite detectar fallos de red que
no cierran el socket.

### Mensajes Engine ↔ Monitor

El Monitor es el servidor y el Engine se conecta a él.

| Petición del Monitor | Respuesta del Engine |
| --- | --- |
| `ID#<ws_id>` (tras confirmar el registro con CENTRAL) | `OK` |
| `CHECK` (cada segundo) | `OK` o `KO` |

El Engine permite pulsar una tecla para responder `KO`. Si responde `KO` o no
responde, el Monitor envía `AVERIA` a CENTRAL. Cuando vuelve a responder `OK`,
envía `RESUELTA`.

## 4. Eventos de Kafka

Los mensajes se envían en JSON.

| Topic | Productor → Consumidor | Contenido |
| --- | --- | --- |
| `wm.peticiones` | FO o Engine → CENTRAL | Solicitudes de riego, del operario o del menú de la estación. |
| `wm.ordenes` | CENTRAL → Engine | Órdenes para una estación. Cada Engine usa su propio grupo de consumo y descarta las que no son suyas. |
| `wm.eventos_ws` | Engine → CENTRAL | Inicio, telemetría cada segundo y fin del riego. |
| `wm.notificaciones` | CENTRAL → FO | Mensajes para un operario. Cada FO filtra por su `fo_id`. |
| `wm.estado_red` | CENTRAL → FO | Lista de estaciones con su estado. |

Ejemplos:

```json
{"tipo": "SOLICITUD", "peticion_id": "FO1-7", "origen": "FO", "fo_id": "FO1", "ws_id": "WS-04", "duracion_s": 30}
{"tipo": "INICIAR_RIEGO", "ws_id": "WS-04", "riego_id": 12, "fo_id": "FO1", "duracion_s": 30}
{"tipo": "PARAR", "ws_id": "WS-04", "motivo": "FUGA"}
{"tipo": "TELEMETRIA", "ws_id": "WS-04", "riego_id": 12, "caudal_lpm": 14.2, "volumen_l": 3.5}
{"tipo": "RIEGO_FINALIZADO", "ws_id": "WS-04", "riego_id": 12, "motivo": "TIEMPO", "volumen_l": 7.1, "duracion_s": 30}
{"tipo": "PASO", "fo_id": "FO1", "ws_id": "WS-04", "texto": "Comprobando disponibilidad..."}
```

Tipos de orden: `INICIAR_RIEGO`, `PARAR`, `BLOQUEAR` y `ACTIVAR`. Tipos de
evento del Engine: `RIEGO_INICIADO`, `RIEGO_RECHAZADO`, `TELEMETRIA` y
`RIEGO_FINALIZADO`. Tipos de notificación: `PASO`, `AUTORIZADO`, `DENEGADO`,
`TELEMETRIA` y `RESUMEN`. Un riego cortado por fuga, bloqueo o desconexión
termina con un `RESUMEN` cuyo `motivo` lo indica, así que no hace falta un tipo
`INTERRUMPIDO` aparte. Todas las notificaciones llevan el `peticion_id`, con el
que el operario sabe a qué solicitud se refieren.

Decisiones del ciclo implementado:

- **Reserva**: al autorizar, la estación queda reservada y no acepta otra
  solicitud hasta que el riego termina o se cancela.
- **Plazo**: si el Engine no confirma `RIEGO_INICIADO` en 5 s, CENTRAL deniega
  la solicitud y envía `PARAR` por si la válvula se abre tarde.
- **Cortes de CENTRAL**: ante `AVERIA`, desconexión del Monitor o bloqueo,
  CENTRAL cierra el riego sin esperar al Engine (podría estar caído) y envía
  `PARAR`. El `RIEGO_FINALIZADO` que llegue después solo corrige en la BD el
  volumen y la duración reales (apartado 10).
- **Solicitudes caducadas**: las que llevan más de 30 s en Kafka (por ejemplo,
  enviadas mientras CENTRAL estaba caída) se deniegan en lugar de regar tarde.
- **Estado de la red**: cada 2 s CENTRAL publica en `wm.estado_red` la lista de
  estaciones con su estado.

El Engine rechaza un `INICIAR_RIEGO` con motivo `SIN_MONITOR`, `FUGA`,
`BLOQUEADA`, `OCUPADA` u `ORDEN_INVALIDA`. CENTRAL ya lo comprueba antes, pero el Engine es quien
controla la electroválvula y no abre si no puede regar:

```json
{"tipo": "RIEGO_RECHAZADO", "ws_id": "WS-04", "riego_id": 12, "motivo": "FUGA"}
```

### Recorrido de un riego

```text
FO ──SOLICITUD──▶ CENTRAL ──PASO "recibida"──▶ FO
                  CENTRAL comprueba: operario válido y WS DISPONIBLE
                  ├─ no ─▶ DENEGADO ──▶ FO
                  └─ sí ─▶ INICIAR_RIEGO ──▶ Engine ──RIEGO_INICIADO──▶ CENTRAL ──AUTORIZADO──▶ FO
                           (sin respuesta del Engine en 5 s ──▶ DENEGADO)
Engine ──TELEMETRIA (cada 1 s)──▶ CENTRAL ──▶ panel y FO
Engine ──RIEGO_FINALIZADO──▶ CENTRAL ──RESUMEN──▶ FO, y se guarda en la BD
```

La telemetría pasa por CENTRAL para que cada operario reciba solo los datos de
la estación que le presta servicio (punto 10 del enunciado).

## 5. Base de datos (SQLite)

```sql
CREATE TABLE operarios (
    id TEXT PRIMARY KEY,
    nombre TEXT NOT NULL
);

CREATE TABLE estaciones (
    id TEXT PRIMARY KEY,
    ubicacion TEXT NOT NULL,
    bloqueada INTEGER NOT NULL DEFAULT 0,
    ultima_conexion TEXT NOT NULL
);

CREATE TABLE riegos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ws_id TEXT NOT NULL REFERENCES estaciones(id),
    fo_id TEXT REFERENCES operarios(id),  -- NULL si el origen no es FO
    origen TEXT NOT NULL,                 -- FO | WS | CENTRAL
    inicio TEXT,
    fin TEXT,
    duracion_s INTEGER,
    volumen_l REAL,
    motivo_fin TEXT                       -- TIEMPO | MANUAL | FUGA | BLOQUEO | DESCONEXION
);
```

- Una estación con ID nuevo se da de alta al registrarse. Si ya existe, se
  actualiza su ubicación y fecha de conexión, conservando su bloqueo.
- Los operarios se administran con `scripts/gestionar_bd.py`. La consulta de
  existencia está implementada; la denegación de peticiones desconocidas se
  conectará al implementar FO y la lógica de riego.
- El esquema ejecutable en `common/base_datos.py` añade restricciones de claves
  foráneas, origen, duración y volumen. La tabla del historial se crea, pero
  todavía no se escriben riegos reales.
- El único fichero SQLite se comparte con el script de administración. Los
  demás módulos de la simulación no acceden directamente a la base.

## 6. Panel de CENTRAL

CENTRAL se despliega en la nube (AWS), donde no hay una terminal interactiva. Por eso
el panel es **web**, servido por la propia CENTRAL (`WM_Central/panel_web.py` y
`WM_Central/panel/index.html`). Se usa el servidor HTTP de la biblioteca estándar
(`http.server`) en lugar de Flask: basta para dos rutas y no añade dependencias.

- La página consulta `GET /api/estado` cada segundo y muestra una tarjeta por
  estación con su color, ubicación y, si está regando, caudal, volumen,
  operario y tiempo.
- Botones para iniciar riego, bloquear o activar una estación o todas
  (`POST /api/orden`, con `{"accion", "ws_id" o null, "duracion_s"}`).
- Registro de los últimos 100 eventos de CENTRAL, que también se escriben en
  consola para verlos en los logs.
- Los textos que llegan de las estaciones se muestran con `textContent`, nunca
  como HTML: una ubicación maliciosa no puede inyectar código en el panel.
- El panel no tiene autenticación en esta versión: cualquiera que alcance el
  puerto puede dar órdenes.

CENTRAL necesita dos puertos públicos: HTTP para el panel (8080, opción
`--http`; `--http 0` lo desactiva) y TCP para los Monitores (9999).

## 7. Argumentos de las aplicaciones

```text
WM_Central <puerto_sockets> <kafka_ip:puerto> [--http 8080] [--db wm.db]
WM_WS_M    <central_ip> <central_puerto> <ws_id> <ubicacion> --puerto-engine <puerto>
WM_WS_E    <kafka_ip:puerto> <monitor_ip> <monitor_puerto>
WM_FO      <kafka_ip:puerto> <fo_id> [--fichero peticiones.txt]
```

El enunciado indica los argumentos mínimos. Se añade la ubicación al Monitor
porque la estación la envía al registrarse. El puerto para el Engine es una
opción y no el primer argumento, para no cambiar los comandos ya documentados
del Monitor. Sin `--puerto-engine`, el Monitor no vigila ningún Engine.

Con `--puerto-engine`, la salud empieza en KO: hasta que el Engine conecta y
responde, CENTRAL ve la estación con avería. Si el Engine responde `KO`, no
responde o se desconecta, el Monitor envía `AVERIA`; cuando vuelve a responder
`OK`, envía `RESUELTA`.

## 8. Fichero de peticiones del operario

El enunciado anuncia un formato, pero el documento no lo incluye. Se usa una
línea por petición con el ID de la estación y la duración en segundos:

```text
WS-04;20
WS-05;15
WS-99;10
```

El FO envía cada petición, espera a que termine (con éxito o con error), espera
4 segundos y pasa a la siguiente. La duración en segundos agiliza las pruebas.

## 9. Simulación del riego

- Caudal aleatorio entre 10 y 20 L/min.
- Cada segundo se suma al volumen acumulado la parte correspondiente
  (`caudal / 60` litros).
- El riego termina al agotar su duración, por el menú de la estación, por una
  fuga, por un bloqueo o por una desconexión.

## 10. Resiliencia

Cada caída solo afecta al servicio del componente caído. CENTRAL publica
`ESTADO_RED` cada 2 s; FO y Engine lo usan como latido (`common/latido.py`): si
pasan 6 s sin recibirlo muestran «Imposible conectar con WM_Central» y no
envían solicitudes nuevas hasta que vuelve.

| Cae | Qué pasa | Al recuperarse |
| --- | --- | --- |
| Monitor | CENTRAL deja de recibir PING: la WS pasa a DESCONECTADA y corta el riego. El Engine pierde el socket: cierra la válvula, se muestra AVERIADA y rechaza riegos, también los manuales (`SIN_MONITOR`). | El Monitor se registra de nuevo y el Engine vuelve a operar con el primer CHECK. |
| Engine | El Monitor no recibe respuesta al CHECK y envía `AVERIA`: la WS pasa a FUGA y CENTRAL cierra el riego con la última telemetría y avisa al FO. El Engine guarda cada segundo el riego en curso en `data/engine_<WS>.json`. | Al recibir su ID, el Engine lee ese fichero y envía un `RIEGO_FINALIZADO` con `"recuperado": true` y lo realmente regado. CENTRAL corrige la BD y envía al FO un `RESUMEN` con el estado final. |
| FO | El riego sigue. | El FO usa un grupo de Kafka fijo (`fo-<id>`): recibe lo que se le envió mientras estaba cerrado (sin la telemetría atrasada) y ve el resultado de su servicio. |
| CENTRAL | Las WS terminan el riego en curso y se paran. No se admiten solicitudes nuevas: FO y Engine las rechazan, y las que se quedaron en Kafka caducan a los 30 s. El Monitor reintenta la conexión. | Recupera de SQLite las estaciones. Si llega telemetría de un riego abierto en la BD, vuelve a seguirlo (REGANDO). Si llega su `RIEGO_FINALIZADO`, lo cierra y envía el `RESUMEN` al operario. |

Si Kafka no confirma un `RIEGO_FINALIZADO`, el Engine lo guarda como pendiente
(también en disco) y lo reenvía cada segundo hasta que se entrega. Las órdenes
`INICIAR_RIEGO` con más de 5 s en Kafka se ignoran: CENTRAL ya las canceló por
falta de respuesta.

**Consumo de Kafka.** Cada módulo usa su propio grupo (`central`,
`engine-<WS>`, `fo-<id>`) y es su único consumidor, así que no se suscribe: se
asigna directamente todas las particiones (`crear_consumidor` en
`common/kafka_utils.py`). Con suscripción, un proceso que vuelve tras una caída
esperaba más de 10 s a que Kafka diera por muerta la sesión anterior y le
repartiera el grupo, y perdía las órdenes de ese intervalo. El grupo solo guarda
hasta dónde se ha leído. Los topics se crean al conectar si no existen.
CENTRAL confirma la lectura (`commit`) después de procesar cada lote: si cae a
mitad, al volver relee lo pendiente.

Comprobado el 29 de octubre de 2026 con Kafka 3.9.1 real: riego completo,
denegación de una WS inexistente y las cuatro caídas de la tabla anterior.
