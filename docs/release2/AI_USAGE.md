# Release 2 · Uso de inteligencia artificial y prompts

El enunciado exige: «Si se utilizan asistentes de IA, deberán incluirse los
prompts utilizados. Las herramientas deben usarse como apoyo; la falta de
dominio absoluto sobre el código entregado supondrá no superar la prueba de
evaluación» (PDF, p. 9). Este registro complementa `docs/USO_IA.md`, que
describe el uso de IA en Release 1 y no se modifica.

## Normas del equipo

1. Cada sesión con IA añade una entrada a este fichero: fecha, herramienta, prompt literal, Issue, qué se generó y cómo se revisó.
2. Los prompts se copian **sin secretos** (credenciales, claves, tokens, API keys, IPs privadas de la instancia). Si un prompt los contenía, se sustituyen por `<OMITIDO>` y se indica.
3. Ningún cambio generado por IA se fusiona sin revisión del otro integrante y sin las pruebas en verde.
4. Antes de la entrega, ambos integrantes deben poder explicar cualquier línea del código ([#34](https://github.com/aggg15/Sistemas-Distribuidos/issues/34)).

## Plantilla de entrada

```markdown
### AAAA-MM-DD · Issue #N · Herramienta (modelo)
**Prompt:** (literal o enlace a la sección de prompts)
**Resultado:** qué generó o modificó la IA (ficheros, Issues, PR).
**Revisión humana:** quién revisó, qué se cambió y qué pruebas se ejecutaron.
```

## Registro

### 2026-11-17 · Planificación de Release 2 · Claude Code (Claude Opus 5.5)

**Prompt:** el prompt completo se reproduce en la sección «Prompt 1».

**Resultado:**

- Lectura del PDF de Release 2 (9 páginas, incluidas la Figura 1 y las capturas de OpenWeather) y análisis del repositorio en `main` (`07a4dbd`): código, documentación, workflow, scripts y pruebas (134 pruebas en verde con `kafka-python`).
- Creación en GitHub de 13 etiquetas nuevas (se reutilizaron `bug`, `documentation` y `enhancement`), 5 milestones, 34 Issues de tarea (#1–#34) y 10 épicas (#35–#44), con sub-issues y relaciones *blocked by*.
- Redacción de `docs/release2/REQUIREMENTS.md`, `ARCHITECTURE.md`, `IMPLEMENTATION_PLAN.md`, `DEPENDENCIES.md`, `TEST_PLAN.md`, este fichero y la sección de Release 2 del README.
- No se implementó ninguna funcionalidad de Release 2 ni se modificó código de Release 1.
- No se creó el tablero de GitHub Projects: el token de `gh` no tiene el permiso `project` (ver «Pendientes de la sesión»).

**Revisión humana:** pendiente. El equipo debe revisar especialmente las
decisiones técnicas de `ARCHITECTURE.md` §4 (Fernet, TLS sobre sockets, Front
en Python en lugar de Node.js, `wm.estado_red` en claro) antes de empezar los
Issues afectados.

**Pendientes de la sesión:**

- Tablero de Projects. Tras `gh auth refresh -s project`, se puede crear con:

  ```bash
  gh project create --owner aggg15 --title "Water Management · Release 2"
  # Añadir el campo Status con Backlog, Ready, In Progress, In Review, Done
  # desde la interfaz web (Settings del proyecto) y después:
  for n in $(seq 1 44); do
    gh project item-add <NUMERO_PROYECTO> --owner aggg15 \
      --url https://github.com/aggg15/Sistemas-Distribuidos/issues/$n
  done
  ```

### 2026-11-17 · Issue #24 · Claude Code (Claude Opus 5.5)

**Prompt:** «Por cual deberiamos empezar?» (respuesta: #24, luego #1 y #2) y, a
continuación, «Hazlo».

**Resultado:** PR #46. Job `tests` (Python 3.9 y 3.12) en
`.github/workflows/deploy.yml`, que se ejecuta en push a `main` y en pull
requests; el job `build` pasa a depender de él y solo publica en push a `main`.
Se actualizaron `README.md` y `docs/DESPLIEGUE_AWS.md`. Verificado en GitHub
Actions: en verde con el cambio, en rojo con una prueba rota a propósito
(commit revertido en el mismo PR) y de nuevo en verde tras el revert. `build`
aparece como *skipped* en los PR.

**Revisión humana:** pendiente (revisar el PR #46 y fusionarlo con *squash*).

### 2026-11-17 · Issues #1 y #2 · Claude Code (Claude Opus 5.5)

**Prompt:** «Haz lo que tengas que hacer» (tras proponer fusionar #45 y #46 y
continuar con #1 y #2).

**Resultado:** la IA intentó fusionar #46 y el permiso fue denegado, así que
#45 y #46 siguen abiertos. Se implementaron:

- PR #47 (#1): esquema de Release 2 con migración desde R1, tabla `auditoria`,
  métodos de registro, claves, alertas y auditoría; listados sin secretos;
  20 pruebas nuevas.
- PR #48 (#2): `common/seguridad.py` (PBKDF2, Fernet, contextos TLS),
  `common/certificados.py` y `scripts/generar_certificados.py` (en Python en
  lugar de openssl), `cryptography==50.0.2`, exclusiones en `.gitignore` y
  `.dockerignore`; 21 pruebas nuevas.

Batería completa en verde con Python 3.14 y 3.9.6 en ambas ramas.

**Revisión humana:** pendiente (PR #47 y #48).

### 2026-11-27 · Issue #6 · Claude Code (Claude Opus 5.5)

**Prompt:** «Te he subido una carpeta de para tener en cuenta la seguridad de
los sockets en python, tenlo en cuenta a la hora de resolver la práctica, haz
los cambios necesarios, si es necesario rehaz el commit, y dime que tengo que
hacer en pocas lineas, se claro y conciso». La carpeta es el ejemplo de
sockets seguros de la asignatura (`server.py`, `cliente.py` y un `cert.pem`),
que no se versiona.

**Resultado:** canal TLS entre CENTRAL y el Monitor siguiendo el ejemplo
(`SSLContext` + `wrap_socket`), pero verificando el certificado en el cliente
en lugar de `_create_unverified_context()`. Opciones `--cert/--key/--ca/--sin-tls`,
certificados montados en Docker y generados por `deploy/arrancar_aws.sh`,
`tests/test_tls_sockets.py` y `docs/release2/SOCKETS_SEGUROS.md`.

**Revisión humana:** pendiente. Se ejecutaron las pruebas con Python 3.9 y
3.14, CENTRAL y el Monitor reales por TLS en local y CENTRAL en Docker.

## Prompts

### Prompt 1 · Planificación de Release 2 (2026-11-17)

````markdown
# PROMPT — Análisis, descomposición y planificación de Water Management Release 2

## 1. Rol y objetivo

Actúa como un **arquitecto de software sénior, ingeniero de sistemas distribuidos y gestor técnico de proyectos**, especializado en Python, REST, seguridad, Docker, Kafka, SQLite, despliegues cloud y GitHub.

Tienes acceso al sistema de archivos del proyecto actual, su código fuente, documentación, historial de Git y repositorio remoto de GitHub.

Tu objetivo es analizar el documento `Practica_SD2627_WaterManagement_Release2.pdf`, contrastarlo exhaustivamente con el proyecto existente de Water Management (Release 1) y **convertir todos los requisitos de la Release 2 en un plan de desarrollo estructurado, trazable y publicado en GitHub**.

No debes implementar las nuevas funcionalidades todavía. Tu tarea inicial es exclusivamente de análisis, planificación, organización y documentación.

## 2. Análisis del proyecto actual

Antes de procesar los requisitos de la Release 2:

1. Explora recursivamente el repositorio.
2. Identifica todos los módulos existentes y sus responsabilidades.
3. Analiza la arquitectura y las comunicaciones entre componentes.
4. Examina el sistema de sockets, Kafka, SQLite y cualquier API existente.
5. Identifica los mecanismos de autenticación, configuración, persistencia y gestión de errores.
6. Examina Docker, Docker Compose, GitHub Actions y los procedimientos de despliegue.
7. Lee el README, documentación técnica y pruebas existentes.
8. Revisa las ramas, los commits relevantes y los Issues ya abiertos.
9. Identifica las funcionalidades de Release 1 que deben conservarse.

No presupongas que una funcionalidad está implementada por el simple hecho de existir un archivo o una clase.

**Distingue entre implementación existente, implementación parcial, funcionalidad no implementada y funcionalidad que requiere verificación.**

No modifiques ni elimines funcionalidades de Release 1.

## 3. Lectura exhaustiva del enunciado

Lee íntegramente el archivo:

`Practica_SD2627_WaterManagement_Release2.pdf`

Debes analizar sus nueve páginas, incluyendo diagramas, imágenes, notas, condiciones obligatorias, funcionalidades opcionales, requisitos de despliegue y entregables.

Presta especial atención a:

- WM_Registry: registro, baja y autenticación de estaciones mediante REST y canal seguro.
- WM_Central: autenticación, gestión y revocación de claves, control de estaciones y auditoría.
- WM_WS_M y WM_WS_E: registro seguro, autenticación y cifrado de mensajes.
- API_Central: exposición de operaciones REST y recepción de alertas meteorológicas.
- WM_W: integración con OpenWeather, consulta cada cuatro segundos y gestión de alertas.
- Front: dashboard web conectado a API_Central.
- Persistencia y compatibilidad con los componentes existentes.
- Despliegue distribuido entre cloud y dos equipos de laboratorio.
- Documentación, evidencias y memoria de prácticas.

No confundas requisitos obligatorios con mejoras opcionales.

Cada requisito deberá tener una referencia precisa a su página y apartado del PDF.

## 4. Matriz de trazabilidad

Genera un archivo:

`docs/release2/REQUIREMENTS.md`

Incluye una tabla con:

| ID | Requisito | Página PDF | Módulo | Estado actual | Prioridad | Issue |
|---|---|---|---|---|---|---|
| R2-001 | Descripción | Página | Componente | Estado | Prioridad | Referencia |

Los identificadores deben ser únicos y estables.

Estados permitidos:

- Implementado y verificado.
- Implementado parcialmente.
- Pendiente.
- Requiere investigación.
- No aplicable, con justificación.

Distingue los requisitos obligatorios de los opcionales.

No omitas ningún requisito funcional, técnico, de seguridad, despliegue o evaluación.

## 5. Descomposición en tareas

Divide la Release 2 en **épicas, tareas y subtareas**, organizadas por componentes y dependencias técnicas.

Como mínimo, contempla estas épicas:

### Épica 1 — WM_Registry
Registro y baja de estaciones, API REST, comunicación segura, generación y entrega de credenciales.

### Épica 2 — Autenticación y seguridad
Autenticación de estaciones, claves simétricas individuales, cifrado de mensajes, revocación y renovación de claves.

### Épica 3 — WM_Central
Adaptaciones del núcleo, integración de servicios, control de estaciones y compatibilidad con Release 1.

### Épica 4 — API_Central
Endpoints REST, consultas del estado del sistema, operaciones de control e integración con otros módulos.

### Épica 5 — Weather Control Office
Integración con OpenWeather, modificación dinámica de localizaciones, detección de lluvia y heladas, suspensión y restauración de estaciones.

### Épica 6 — Frontend
Dashboard web, monitorización de estaciones, operarios, riegos, autenticación, clima, errores y alertas.

### Épica 7 — Auditoría
Registro estructurado de eventos operacionales y de seguridad, identificación del origen, fecha, acción y parámetros relevantes.

### Épica 8 — Integración y pruebas
Pruebas unitarias, integración, escenarios completos, recuperación de errores y regresión de Release 1.

### Épica 9 — Despliegue distribuido
Despliegue cloud, configuración de equipos de laboratorio, conectividad, variables de entorno y validación de la arquitectura completa.

### Épica 10 — Documentación y entrega
Memoria técnica, guía de despliegue, capturas, código fuente, documentación de IA y preparación de la evaluación.

Puedes dividir, combinar o ampliar estas épicas si la arquitectura real lo justifica.

## 6. Creación de GitHub Issues

Utiliza GitHub CLI (`gh`), la API de GitHub o las herramientas disponibles para publicar la planificación en el repositorio remoto existente.

No crees un repositorio nuevo.

Por cada tarea, crea un Issue independiente con la siguiente estructura:

**Título:**
`[R2][MÓDULO] Descripción concreta de la tarea`

**Contenido:**

### Objetivo
Qué se pretende conseguir y por qué es necesario.

### Referencia al enunciado
Página, apartado y requisito identificado.

### Estado actual
Qué existe en el repositorio y qué falta por desarrollar.

### Implementación prevista
Descripción técnica de los cambios necesarios, incluyendo los componentes y archivos afectados cuando puedan identificarse.

### Subtareas
Checklist de actividades concretas, verificables y suficientemente pequeñas.

### Criterios de aceptación
Condiciones objetivas que permiten considerar la tarea completada.

### Pruebas necesarias
Pruebas unitarias, de integración, seguridad o despliegue que correspondan.

### Dependencias
Otros Issues que deben completarse previamente.

### Riesgos
Posibles problemas de compatibilidad, seguridad o integración.

### Estimación
Complejidad relativa: baja, media, alta o muy alta.

### Clasificación
Obligatorio u opcional.

Cada Issue debe ser suficientemente independiente para que otro agente de IA pueda implementarlo sin tener que reinterpretar todo el enunciado.

No generes tareas duplicadas ni Issues excesivamente genéricos.

## 7. Organización de GitHub

Organiza los Issues mediante:

**Milestones:**
- Release 2 — Arquitectura y seguridad
- Release 2 — Servicios REST
- Release 2 — Meteorología y frontend
- Release 2 — Integración y pruebas
- Release 2 — Despliegue y entrega

**Labels:**
- `release-2`
- `backend`
- `frontend`
- `security`
- `rest-api`
- `weather`
- `database`
- `testing`
- `deployment`
- `documentation`
- `bug`
- `enhancement`
- `mandatory`
- `optional`
- `blocked`

Reutiliza los labels y milestones existentes cuando corresponda.

Si GitHub Projects está disponible, organiza las tareas en un tablero con los estados:

`Backlog → Ready → In Progress → In Review → Done`

No elimines ni alteres Issues, milestones o proyectos anteriores.

## 8. Plan de implementación

Genera:

`docs/release2/IMPLEMENTATION_PLAN.md`

Debe incluir:

1. Resumen de la arquitectura existente.
2. Arquitectura objetivo de Release 2.
3. Diagrama Mermaid de componentes y comunicaciones.
4. Orden de implementación recomendado.
5. Dependencias entre módulos.
6. Ruta crítica del proyecto.
7. Riesgos técnicos.
8. Estrategia de pruebas.
9. Estrategia de despliegue.
10. Checklist final de evaluación.

Prioriza las tareas considerando dependencias, complejidad y riesgo de integración.

La fecha de entrega indicada en el PDF corresponde a la semana del 13 de diciembre de 2026.

No inventes fechas intermedias como si fueran oficiales.

## 9. Compatibilidad y decisiones técnicas

Respeta las tecnologías ya utilizadas en el proyecto siempre que permitan cumplir el enunciado.

No introduzcas frameworks, servicios cloud o dependencias nuevas sin justificar su necesidad.

Para cualquier decisión técnica relevante, documenta:

- Alternativas consideradas.
- Solución recomendada.
- Ventajas e inconvenientes.
- Impacto sobre Release 1.
- Riesgos de seguridad.
- Complejidad de implementación.

Especialmente, analiza el mecanismo de cifrado simétrico, el intercambio seguro de claves, la gestión de credenciales y las comunicaciones entre servicios.

No publiques credenciales, claves privadas, tokens ni secretos en GitHub.

## 10. Documentación de la planificación

Crea también:

`docs/release2/ARCHITECTURE.md`

`docs/release2/DEPENDENCIES.md`

`docs/release2/TEST_PLAN.md`

`docs/release2/AI_USAGE.md`

El último archivo deberá registrar el uso de este prompt y la intervención de IA, dado que el enunciado exige documentar los prompts utilizados.

Actualiza el README principal para incorporar una sección de Release 2, enlazando la documentación y los Issues correspondientes.

No sobrescribas documentación existente de Release 1.

## 11. Publicación en GitHub

Antes de publicar:

1. Comprueba el estado de Git y la rama activa.
2. Verifica el repositorio remoto.
3. Comprueba permisos de escritura.
4. Busca Issues existentes para evitar duplicados.
5. Prepara los documentos de planificación.
6. Valida los enlaces y referencias.

Después:

1. Crea los labels necesarios.
2. Crea los milestones.
3. Publica los Issues.
4. Establece las referencias de dependencia.
5. Publica la documentación mediante una rama específica y un Pull Request.
6. No hagas merge automáticamente a la rama principal.
7. Verifica que todos los Issues y documentos sean accesibles.

No utilices `push --force`, no borres ramas y no modifiques secretos ni configuraciones de producción.

Si alguna operación requiere permisos que no tienes, informa de ello y prepara los comandos necesarios, sin afirmar que se ha realizado.

## 12. Informe final obligatorio

Al finalizar, presenta:

- Resumen de la arquitectura encontrada.
- Número de requisitos identificados.
- Número de requisitos ya implementados, parciales y pendientes.
- Número de épicas y tareas creadas.
- Enlaces a los Issues y milestones.
- Enlace al Pull Request de documentación.
- Principales dependencias técnicas.
- Riesgos críticos.
- Orden de implementación recomendado.
- Primera tarea que debería desarrollarse.

Incluye una tabla que relacione todos los requisitos de Release 2 con sus Issues correspondientes.

**Condición fundamental:** no declares completada la planificación hasta haber comprobado que todos los requisitos obligatorios del PDF tienen cobertura, que no existen duplicados y que los enlaces publicados funcionan.

## 13. Restricción final

En esta ejecución NO debes programar la Release 2.

Tu responsabilidad es dejar el proyecto completamente analizado, dividido, documentado y organizado en GitHub para que las futuras sesiones de desarrollo puedan resolver las tareas una a una, de forma incremental, verificable y sin romper Release 1.

La implementación comenzará únicamente cuando se seleccione expresamente un Issue concreto.
````
