# Release 2 · Plan de pruebas

## 1. Punto de partida

- Batería de R1: `python -m unittest` → **134 pruebas OK** (17/11/2026, `main` en `07a4dbd`, con `kafka-python` instalado; sin él se omiten 61).
- Guía manual de R1: `docs/PRUEBAS_SQLITE_KAFKA.md` (P01–P16) y escenarios de caída de `docs/DISENO.md` §10, verificados el 29/10/2026 con Kafka 3.9.1.
- CI: actualmente no ejecuta pruebas; lo corrige [#24](https://github.com/aggg15/Sistemas-Distribuidos/issues/24).

## 2. Niveles y herramientas

| Nivel | Herramienta | Entorno | Cuándo |
|---|---|---|---|
| Unitarias | `unittest` (como en R1) y `node --test` (Registry y Front) | Sin red externa: servidores en hilo y puerto 0, BD temporal, OpenWeather simulado | Cada PR (CI) |
| Seguridad | `unittest` + pruebas manuales (`tcpdump`/Wireshark, `kafka-console-consumer`, `curl`) | Local | Al cerrar [#9](https://github.com/aggg15/Sistemas-Distribuidos/issues/9), [#10](https://github.com/aggg15/Sistemas-Distribuidos/issues/10) y antes del despliegue |
| Integración | `docker compose` + scripts | Local, todos los módulos | Fase 5 y antes de cada despliegue |
| Regresión R1 | Batería completa + escenarios de caída | Local y AWS | Cada fase |
| Despliegue | Checklist ([IMPLEMENTATION_PLAN.md §10](IMPLEMENTATION_PLAN.md#10-checklist-final-de-evaluación)) | Cloud + PC1 + PC2 | [#30](https://github.com/aggg15/Sistemas-Distribuidos/issues/30) |

Reglas: ninguna prueba usa la API key real de OpenWeather ni secretos reales;
los certificados de prueba se generan en directorios temporales; las pruebas
que necesitan Kafka real se omiten (como en R1) si no está disponible.

## 3. Pruebas unitarias por tarea

| Tarea | Fichero previsto | Casos mínimos |
|---|---|---|
| [#1](https://github.com/aggg15/Sistemas-Distribuidos/issues/1) | `tests/test_base_datos_r2.py` | Migración desde esquema R1; alta, baja, re-alta; claves; alerta |
| [#2](https://github.com/aggg15/Sistemas-Distribuidos/issues/2) | `tests/test_seguridad.py` | Hash/verificación; Fernet ida y vuelta; manipulación; clave errónea; TTL; handshake TLS |
| [#3](https://github.com/aggg15/Sistemas-Distribuidos/issues/3) | `WM_Registry/test/*.test.js` (`npm test`) y `tests/test_registry.py` | PUT/GET/DELETE/POST; 400/401/404/409/413; altas simultáneas; auditoría; hash verificable desde Python y viceversa; HTTPS real con la CA del proyecto, otra CA y HTTP plano rechazados; espera a que CENTRAL cree la BD. Manual: colección de Postman |
| [#4](https://github.com/aggg15/Sistemas-Distribuidos/issues/4), [#8](https://github.com/aggg15/Sistemas-Distribuidos/issues/8) | `tests/test_monitor.py` | Cliente Registry; fichero de credencial; AUTENTICAR; CLAVE/REVOCADA al Engine |
| [#6](https://github.com/aggg15/Sistemas-Distribuidos/issues/6), [#7](https://github.com/aggg15/Sistemas-Distribuidos/issues/7) | `tests/test_comunicaciones.py` | Canal sobre TLS; motivos de KO; claves distintas por WS |
| [#9](https://github.com/aggg15/Sistemas-Distribuidos/issues/9) | `tests/test_riegos.py`, `tests/test_engine.py` | Sobres cifrados en ambos sentidos; descarte de mensajes inválidos |
| [#10](https://github.com/aggg15/Sistemas-Distribuidos/issues/10) | `tests/test_revocacion.py` | Revocar → FUERA_SERVICIO → reautenticar con clave nueva |
| [#14](https://github.com/aggg15/Sistemas-Distribuidos/issues/14), [#18](https://github.com/aggg15/Sistemas-Distribuidos/issues/18) | `tests/test_auditoria.py` | Campos, filtro de secretos, consulta, tolerancia a fallos |
| [#15](https://github.com/aggg15/Sistemas-Distribuidos/issues/15) | `tests/test_riegos.py`, `tests/test_engine.py` | Alerta con y sin riego; cancelación; combinación con bloqueo |
| [#12](https://github.com/aggg15/Sistemas-Distribuidos/issues/12), [#16](https://github.com/aggg15/Sistemas-Distribuidos/issues/16), [#17](https://github.com/aggg15/Sistemas-Distribuidos/issues/17) | `tests/test_api_central.py` | Cada endpoint; tokens; CORS; compatibilidad con `/api/estado` |
| [#19](https://github.com/aggg15/Sistemas-Distribuidos/issues/19), [#20](https://github.com/aggg15/Sistemas-Distribuidos/issues/20), [#21](https://github.com/aggg15/Sistemas-Distribuidos/issues/21) | `tests/test_wm_w.py` | JSON de la p. 8; unidades; errores 401/404/429; recarga de fichero; detección de cambios |
| [#22](https://github.com/aggg15/Sistemas-Distribuidos/issues/22) | `WM_Front/test/front.test.js` (`npm test`) | Proxy de GET; 405 en POST/PUT/DELETE; 502 con API_Central caída; cabeceras de seguridad; sin `innerHTML` |

## 4. Casos de seguridad (SEC) — [#25](https://github.com/aggg15/Sistemas-Distribuidos/issues/25)

| ID | Caso | Resultado esperado | Req. |
|---|---|---|---|
| SEC-01 | Alta en el Registry por HTTP plano | Conexión rechazada | R2-010 |
| SEC-02 | Monitor con CA distinta contra Registry y CENTRAL | Handshake fallido, error claro, evento auditado en servidor | R2-010, R2-031 |
| SEC-03 | Captura de tráfico durante alta y autenticación | Ningún ID, credencial ni clave en claro | R2-031 |
| SEC-04 | `AUTENTICAR` con credencial errónea (×5) | `KO#CREDENCIAL_INVALIDA` y 5 eventos `AUTH_KO` con IP | R2-014, R2-026 |
| SEC-05 | WS sin alta / dada de baja | `KO#WS_NO_REGISTRADA` / `KO#WS_DADA_DE_BAJA` | R2-008 |
| SEC-06 | Dos WS autenticadas | Claves distintas | R2-015 |
| SEC-07 | Leer `wm.ordenes` y `wm.eventos_ws` con `kafka-console-consumer` | Solo sobres con tokens ilegibles | R2-016, R2-021 |
| SEC-08 | Inyectar en Kafka un `INICIAR_RIEGO` en claro o cifrado con otra clave | Ignorado y auditado como `MENSAJE_RECHAZADO` | R2-021 |
| SEC-09 | Modificar un byte de un token | Rechazado | R2-021 |
| SEC-10 | Revocar y reutilizar la clave antigua | Mensajes rechazados; WS FUERA_SERVICIO hasta reautenticar | R2-017, R2-018 |
| SEC-11 | Operación de control en API_Central sin token o con token erróneo | 401 y auditoría | R2-028 |
| SEC-12 | Buscar secretos en el repositorio (`git grep` de patrones de claves, `.pem`, `.cred`, API key) | Sin resultados | R2-D01 |
| SEC-13 | Inyección de HTML en ubicación o ciudad mostrada en el Front | Se muestra como texto | R2-040 |

## 5. Escenarios de integración (INT) — [#26](https://github.com/aggg15/Sistemas-Distribuidos/issues/26)

| ID | Escenario | Resultado esperado |
|---|---|---|
| INT-01 | Alta → autenticación → solicitud desde la WS → riego completo | Igual que R1, con mensajes cifrados |
| INT-02 | Solicitud desde el FO a una WS autenticada | AUTORIZADO, telemetría y RESUMEN en el FO |
| INT-03 | Baja durante una sesión activa | La sesión se cierra o se rechaza en la siguiente autenticación; riego cerrado |
| INT-04 | Revocación durante un riego y reautenticación | Riego cortado (BLOQUEO), FUERA_SERVICIO, clave nueva, vuelve a DISPONIBLE |
| INT-05 | WM_W: ciudad bajo 0 °C (simulada) sin riego | WS FUERA_SERVICIO en ≤ 8 s; FO ve el estado; solicitudes denegadas |
| INT-06 | WM_W: alerta durante un riego | El riego termina por TIEMPO; después FUERA_SERVICIO |
| INT-07 | Cancelación de la alerta | WS DISPONIBLE sin reiniciar nada |
| INT-08 | Cambio de ciudad por menú y por fichero en caliente | Efecto en el siguiente ciclo |
| INT-09 | Alerta + bloqueo manual; cancelar alerta | Sigue bloqueada |
| INT-10 | Front: todos los datos de la p. 5 visibles y acciones operativas | Capturas |
| INT-11 | Auditoría tras INT-01…INT-10 | Todas las acciones del catálogo aparecen con fecha, origen e IP |
| INT-12 | OpenWeather no disponible / 429 | WM_W conserva el último estado y lo informa; sin cancelaciones falsas |

## 6. Regresión de Release 1 (REG) — [#26](https://github.com/aggg15/Sistemas-Distribuidos/issues/26)

| ID | Escenario (docs/DISENO.md §10 y guía de R1) | Resultado esperado |
|---|---|---|
| REG-01 | Batería `python -m unittest` | Todo en verde |
| REG-02 | Caída del Monitor durante un riego | DESCONECTADA, riego cortado; vuelve tras reautenticar |
| REG-03 | Caída del Engine durante un riego | FUGA, cierre con última telemetría; al volver, `RIEGO_FINALIZADO` recuperado (cifrado) |
| REG-04 | Caída del FO | El riego sigue; al volver recibe su resumen |
| REG-05 | Caída de CENTRAL | Riegos en curso terminan; al volver recupera estaciones (con sus claves) y riegos abiertos |
| REG-06 | Bloquear/activar desde el panel R1 y desde la API | Igual que en R1 |
| REG-07 | Fichero de peticiones del FO (`WM_FO/peticiones_5_riegos.txt`) | Igual que en R1 |

## 7. Registro de resultados

| Fecha | Commit / versión | Entorno | Casos | Resultado | Responsable | Evidencia |
|---|---|---|---|---|---|---|
| 17/11/2026 | `07a4dbd` | Local (macOS, Python 3.14, kafka-python 3.0.11) | REG-01 | 134 OK | Planificación (IA, revisión pendiente) | Salida de `python -m unittest` |
| 27/11/2026 | rama `feat/r2-registry-front-node` | Local (macOS, Python 3.9.6 y 3.14, Node.js 26) y Docker Compose | REG-01, #3, #22, SEC-01 | Python y Node en verde; Registry + Front + CENTRAL en Docker | IA, revisión pendiente | Salida de `python -m unittest` y `npm test`; PR en GitHub |
