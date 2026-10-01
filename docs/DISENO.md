# Diseño de Water Management (Fase 0)

Este documento recoge las decisiones de diseño tomadas antes de implementar la
práctica completa: estados de las estaciones, protocolo de sockets, eventos de
Kafka, base de datos, panel de CENTRAL y argumentos de cada aplicación.

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
  bytes `0x02` y `0x03` nunca aparecen dentro de un texto UTF-8, y el LRC puede
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
| `REGISTRO#<ws_id>#<ubicacion>` | `OK#<ws_id>` o `KO#<motivo>` |
| `AVERIA#<ws_id>` | `OK` |
| `RESUELTA#<ws_id>` | `OK` |
| `PING#<ws_id>` (cada 2 s) | `OK` |

Si CENTRAL pasa 6 s sin recibir mensajes de un Monitor, o su conexión se cierra,
marca la estación como DESCONECTADA. El `PING` permite detectar fallos de red que
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
notificación: `PASO`, `AUTORIZADO`, `DENEGADO`, `TELEMETRIA`, `RESUMEN` e
`INTERRUMPIDO`.

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
    ultima_conexion TEXT
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
  actualiza su ubicación.
- Los operarios se cargan con un script inicial. Las peticiones de un operario
  desconocido se deniegan.

## 6. Panel de CENTRAL

CENTRAL se despliega en Railway, donde no hay una terminal interactiva. Por eso
el panel es **web**, servido por la propia CENTRAL con Flask:

- La página consulta `GET /api/estado` cada segundo y muestra una tarjeta por
  estación con su color, ubicación y, si está regando, caudal, volumen y operario.
- Botones para iniciar riego, bloquear o activar una estación o todas
  (`POST /api/orden`).
- Registro de eventos, que también se escribe en consola para verlo en los logs.

CENTRAL necesita dos puertos públicos: HTTP para el panel y TCP para los Monitores.

## 7. Argumentos de las aplicaciones

```text
WM_Central <puerto_sockets> <kafka_ip:puerto> [--http 8080] [--db wm.db]
WM_WS_M    <puerto_para_engine> <central_ip> <central_puerto> <ws_id> <ubicacion>
WM_WS_E    <kafka_ip:puerto> <monitor_ip> <monitor_puerto>
WM_FO      <kafka_ip:puerto> <fo_id> [--fichero peticiones.txt]
```

El enunciado indica los argumentos mínimos. Se añade la ubicación al Monitor
porque la estación la envía al registrarse.

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
