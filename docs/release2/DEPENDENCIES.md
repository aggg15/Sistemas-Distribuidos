# Release 2 · Dependencias

## 1. Dependencias entre tareas

Cada flecha `A --> B` significa «B necesita A terminada». Las mismas relaciones
están registradas en GitHub como *blocked by* en cada Issue y como sub-issues de
su épica. Las tareas opcionales aparecen con borde discontinuo.

```mermaid
flowchart LR
    T01["#1 T01<br/>BD"]
    T02["#2 T02<br/>SEGURIDAD"]
    T03["#3 T03<br/>REGISTRY"]
    T04["#4 T04<br/>WS_M"]
    T05["#5 T05<br/>REGISTRY"]:::opt
    T06["#6 T06<br/>CENTRAL"]
    T07["#7 T07<br/>CENTRAL"]
    T08["#8 T08<br/>WS_M"]
    T09["#9 T09<br/>KAFKA"]
    T10["#10 T10<br/>CENTRAL"]
    T11["#11 T11<br/>KAFKA"]:::opt
    T12["#13 T12<br/>CENTRAL"]:::opt
    T13["#14 T13<br/>AUDITORIA"]
    T14["#15 T14<br/>CENTRAL"]
    T15["#12 T15<br/>API_CENTRAL"]
    T16["#16 T16<br/>API_CENTRAL"]
    T17["#17 T17<br/>API_CENTRAL"]
    T18["#18 T18<br/>AUDITORIA"]
    T19["#19 T19<br/>WM_W"]
    T20["#20 T20<br/>WM_W"]
    T21["#21 T21<br/>WM_W"]
    T22["#22 T22<br/>FRONT"]
    T23["#23 T23<br/>FRONT"]
    T24["#24 T24<br/>CI"]
    T25["#25 T25<br/>TESTS"]
    T26["#26 T26<br/>TESTS"]
    T27["#27 T27<br/>DEPLOY"]
    T28["#28 T28<br/>DEPLOY"]
    T29["#29 T29<br/>DEPLOY"]
    T30["#30 T30<br/>DEPLOY"]
    T31["#31 T31<br/>DOCS"]
    T32["#33 T32<br/>DOCS"]
    T33["#32 T33<br/>DOCS"]
    T34["#34 T34<br/>DOCS"]
    T01 --> T03
    T02 --> T03
    T03 --> T04
    T03 --> T05
    T04 --> T05
    T02 --> T06
    T01 --> T07
    T02 --> T07
    T06 --> T07
    T04 --> T08
    T07 --> T08
    T02 --> T09
    T07 --> T09
    T08 --> T09
    T07 --> T10
    T08 --> T10
    T09 --> T10
    T09 --> T11
    T07 --> T12
    T15 --> T12
    T01 --> T13
    T01 --> T14
    T01 --> T15
    T15 --> T16
    T10 --> T16
    T15 --> T17
    T14 --> T17
    T13 --> T18
    T03 --> T18
    T07 --> T18
    T14 --> T18
    T15 --> T18
    T17 --> T21
    T19 --> T21
    T20 --> T21
    T15 --> T22
    T22 --> T23
    T16 --> T23
    T17 --> T23
    T18 --> T23
    T09 --> T25
    T10 --> T25
    T03 --> T25
    T09 --> T26
    T10 --> T26
    T14 --> T26
    T18 --> T26
    T21 --> T26
    T23 --> T26
    T03 --> T27
    T06 --> T27
    T15 --> T27
    T22 --> T27
    T27 --> T28
    T27 --> T29
    T28 --> T30
    T29 --> T30
    T26 --> T30
    T09 --> T31
    T17 --> T31
    T23 --> T31
    T28 --> T31
    T30 --> T32
    T31 --> T32
    T33 --> T32
    T30 --> T34
    classDef opt stroke-dasharray: 5 5
```

Tareas sin dependencias (pueden empezar ya): [#1](https://github.com/aggg15/Sistemas-Distribuidos/issues/1), [#2](https://github.com/aggg15/Sistemas-Distribuidos/issues/2), [#19](https://github.com/aggg15/Sistemas-Distribuidos/issues/19), [#20](https://github.com/aggg15/Sistemas-Distribuidos/issues/20),
[#24](https://github.com/aggg15/Sistemas-Distribuidos/issues/24), [#32](https://github.com/aggg15/Sistemas-Distribuidos/issues/32).

Ruta crítica: [#2](https://github.com/aggg15/Sistemas-Distribuidos/issues/2) → [#6](https://github.com/aggg15/Sistemas-Distribuidos/issues/6) → [#7](https://github.com/aggg15/Sistemas-Distribuidos/issues/7) → [#8](https://github.com/aggg15/Sistemas-Distribuidos/issues/8) → [#9](https://github.com/aggg15/Sistemas-Distribuidos/issues/9) → [#10](https://github.com/aggg15/Sistemas-Distribuidos/issues/10) → [#25](https://github.com/aggg15/Sistemas-Distribuidos/issues/25)/[#26](https://github.com/aggg15/Sistemas-Distribuidos/issues/26) →
[#27](https://github.com/aggg15/Sistemas-Distribuidos/issues/27) → [#28](https://github.com/aggg15/Sistemas-Distribuidos/issues/28) → [#30](https://github.com/aggg15/Sistemas-Distribuidos/issues/30) → [#33](https://github.com/aggg15/Sistemas-Distribuidos/issues/33).

## 2. Dependencias entre módulos

| Módulo | Depende de | Para |
|---|---|---|
| `common/seguridad.py` (nuevo) | `cryptography`, `ssl`, `hashlib`, `secrets` | Credenciales, cifrado Fernet, contextos TLS |
| `common/auditoria.py` (nuevo) | `common/base_datos.py` | Eventos estructurados |
| `common/base_datos.py` | `sqlite3` | Esquema ampliado y migración |
| WM_Registry (nuevo) | seguridad, base_datos, auditoría | Alta, baja y autenticación por HTTPS |
| WM_Central | protocolo, seguridad, base_datos, auditoría, kafka_utils | TLS, autenticación, claves, cifrado, revocación, clima |
| API_Central | WM_Central (mismo proceso), auditoría | REST de consulta, control y alertas |
| Front (nuevo) | API_Central | Dashboard |
| WM_WS_M | protocolo, seguridad, consola, WM_Registry (HTTPS), WM_Central (TLS) | Menú, alta, autenticación, entrega de la clave |
| WM_WS_E | protocolo, seguridad, kafka_utils, WM_WS_M | Cifrado y descifrado de mensajes |
| WM_FO | kafka_utils | Sin cambios funcionales (solo campo `ip`) |
| WM_W (nuevo) | OpenWeather, API_Central, consola | Clima cada 4 s y alertas |

## 3. Dependencias externas

| Dependencia | Estado | Justificación | Tarea |
|---|---|---|---|
| `kafka-python==3.0.11` | Existente | Cliente Kafka de R1 | — |
| `cryptography` (versión fijada al implementarlo) | Nueva | La biblioteca estándar no incluye cifrado simétrico (Fernet/AES) | [#2](https://github.com/aggg15/Sistemas-Distribuidos/issues/2) |
| `openssl` (CLI) | Herramienta de despliegue | Generar CA y certificados con SAN | [#2](https://github.com/aggg15/Sistemas-Distribuidos/issues/2), [#28](https://github.com/aggg15/Sistemas-Distribuidos/issues/28) |
| apache/kafka 3.9.1 (Docker) | Existente | Broker | — |
| OpenWeather Current weather data | Servicio externo nuevo | Exigido por el PDF (p. 5–8); API key gratuita | [#19](https://github.com/aggg15/Sistemas-Distribuidos/issues/19) |
| AWS Labs (EC2) | Existente | Despliegue cloud (p. 9) | [#28](https://github.com/aggg15/Sistemas-Distribuidos/issues/28) |
| Docker Hub + GitHub Actions | Existente | Publicación de la imagen | [#24](https://github.com/aggg15/Sistemas-Distribuidos/issues/24) |
| Node.js | **No se añade** | Recomendado pero no obligatorio (p. 5); ver ARCHITECTURE.md D7 | [#22](https://github.com/aggg15/Sistemas-Distribuidos/issues/22) |
| Flask / FastAPI | **No se añade** | `http.server` basta (ARCHITECTURE.md D4) | — |

## 4. Dependencias de configuración

| Variable | Componente | Ejemplo / valor por defecto | Secreto |
|---|---|---|---|
| `WM_TLS_CERT`, `WM_TLS_KEY` | CENTRAL, Registry | `certs/servidor.pem`, `certs/servidor.key` | La clave sí |
| `WM_CA_CERT` | Monitor, WM_W, Front | `certs/ca.pem` | No |
| `WM_REGISTRY_URL` | Monitor | `https://IP_AWS:8443` | No |
| `WM_REGISTRY_PORT` | Registry | `8443` | No |
| `WM_HTTP_PORT` | API_Central | `8080` (existente) | No |
| `WM_API_TOKEN` | API_Central, Front | — | Sí |
| `WM_W_TOKEN` | API_Central, WM_W | — | Sí |
| `OPENWEATHER_API_KEY` | WM_W | — | Sí |
| `WM_API_CENTRAL_URL` | Front, WM_W | `http://IP_AWS:8080` | No |
| `WM_FRONT_PORT` | Front | `8000` | No |
| Existentes de R1 | todos | `WM_KAFKA_SERVER`, `WM_DB_PATH`, `WM_SOCKET_PORT`... | No |

Los nombres definitivos se fijan en cada Issue y se recogen en un
`.env.example` sin valores ([#27](https://github.com/aggg15/Sistemas-Distribuidos/issues/27)).
