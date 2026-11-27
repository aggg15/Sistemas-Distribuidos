# Release 2 · Sockets seguros entre CENTRAL y los Monitores

Issue [#6](https://github.com/aggg15/Sistemas-Distribuidos/issues/6) ·
requisitos R2-020, R2-021 y R2-031 · decisión D2 de `ARCHITECTURE.md`.

La autenticación de las estaciones sigue siendo por sockets (enunciado, p. 4),
pero el Monitor no puede enviar su identificación en claro. El canal TCP se
envuelve en **TLS** y dentro se mantiene el mismo protocolo
`ENQ/ACK · <STX>DATA<ETX><LRC> · EOT` de Release 1: `common/protocolo.Canal`
funciona igual sobre un `ssl.SSLSocket`.

## 1. Del ejemplo de la asignatura a la práctica

El ejemplo de sockets seguros (`server.py` y `cliente.py`, no versionados) usa
`ssl.SSLContext` + `wrap_socket` en los dos extremos. Se sigue el mismo
esquema con los cambios necesarios para que proteja de verdad:

| Ejemplo | Práctica | Motivo |
| --- | --- | --- |
| `SSLContext(PROTOCOL_TLS_SERVER)` + `load_cert_chain(cert, cert)` | `common.seguridad.contexto_tls_servidor(cert, clave)`; `--cert` admite también un único PEM con certificado y clave, como el ejemplo | Igual, con TLS 1.2 como mínimo |
| `context.wrap_socket(newsocket, server_side=True)` en el bucle de `accept` | `CentralTCP._cifrar`, en el **hilo de cada conexión** | Un cliente lento o que no hable TLS no bloquea al resto |
| `ssl._create_unverified_context()` en el cliente | `contexto_tls_cliente(certs/ca.pem)`: `CERT_REQUIRED` y `check_hostname` | Sin verificar el certificado, un MITM puede hacerse pasar por CENTRAL y leer el ID |
| Certificado autofirmado `CN=ServerPrueba` sin SAN | CA propia + certificado de servidor con la IP pública y `localhost` en el SAN (`scripts/generar_certificados.py`) | Los clientes comprueban que el certificado es de esa IP |
| `send`/`recv(1024)` sueltos | `Canal` con STX/ETX/LRC, ACK/NACK y plazos sobre el `SSLSocket` | El protocolo de Release 1 no cambia |
| `connstream.shutdown(SHUT_RDWR)` | `socket.socket.shutdown(conexion, SHUT_RD)` al parar CENTRAL | Desbloquea el `recv` sin desactivar TLS, así el `EOT` final sale cifrado |
| Errores sin tratar | Fallo de handshake → aviso, cierre y fila `AUTH_KO` (`motivo: HANDSHAKE_TLS`) en `auditoria`; el Monitor no reintenta si el certificado no es de confianza | Trazabilidad y mensajes claros |

El `cliente.py` del ejemplo puede negociar TLS con CENTRAL (no verifica el
certificado), pero CENTRAL lo corta en cuanto envía `HOLA MUNDO` porque no
empieza con `ENQ`.

## 2. Ficheros

| Fichero | Dónde | Se comparte |
| --- | --- | --- |
| `certs/ca.pem` | servidor y cada PC con Monitor | Sí: es pública |
| `certs/ca.key` | solo servidor | **No** (permisos 600) |
| `certs/servidor.pem` | solo servidor | No hace falta |
| `certs/servidor.key` | solo servidor | **No** (permisos 600) |

`certs/` está en `.gitignore` y `.dockerignore`: ningún certificado ni clave
llega a Git ni a la imagen de Docker Hub. En Docker se montan con
`-v .../certs:/app/certs:ro`.

## 3. Uso

```bash
# Servidor (lo hace deploy/arrancar_aws.sh en AWS)
python scripts/generar_certificados.py IP_PUBLICA
python WM_Central/WM_Central.py 9999 IP_KAFKA:9092          # usa certs/servidor.{pem,key}

# PC del laboratorio: copiar certs/ca.pem del servidor y lanzar el Monitor
python WM_WS/WM_WS_M.py IP_PUBLICA 9999 WS-04 "River Park"  # usa certs/ca.pem
```

| Opción | Variable | Módulo | Uso |
| --- | --- | --- | --- |
| `--cert`, `--key` | `WM_TLS_CERT`, `WM_TLS_KEY` | CENTRAL | Otro certificado (por defecto `certs/servidor.*`) |
| `--ca` | `WM_CA_CERT` | Monitor | Otra CA (por defecto `certs/ca.pem`) |
| `--sin-comprobar-nombre` | `WM_TLS_SIN_NOMBRE=1` | Monitor | Verifica la CA pero no la IP (solo Docker local, donde CENTRAL se llama `central`) |
| `--sin-tls` | `WM_SIN_TLS=1` | ambos | Sockets en claro como en Release 1, **solo pruebas** |

TLS está activado por defecto: si faltan los certificados, CENTRAL y el Monitor
**no arrancan** en lugar de pasar a texto en claro sin avisar.

## 4. Pruebas

`tests/test_tls_sockets.py` arranca la CENTRAL real con TLS en localhost:

- El Monitor con la CA del proyecto se registra y envía latidos por TLS 1.2/1.3.
- Un proxy intermedio (MITM pasivo) no ve `REGISTRO`, `PING`, el ID ni la ubicación.
- Al parar CENTRAL, el Monitor recibe el `EOT` cifrado: cierre ordenado.
- Rechazos: otra CA (el Monitor sale con código 1 sin reintentar), certificado
  para otra IP, TLS < 1.2 y Monitor en claro (queda `AUTH_KO` en `auditoria`).
- Sin certificados ni CA, ninguno de los dos arranca en claro.

`tests/test_comunicaciones.py` sigue probando el protocolo sin TLS (regresión de Release 1).
