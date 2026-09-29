# Uso de IA como apoyo consultivo y didáctico

## Objetivo y alcance

Se ha utilizado Codex para consultar dudas, interpretar los materiales de la
asignatura y comprender la comunicación mediante sockets TCP y la concurrencia.
La asistencia incluyó explicaciones paso a paso, una propuesta de implementación
comentada y comprobaciones de funcionamiento.

En particular, la herramienta revisó los ejemplos proporcionados en la asignatura,
explicó la calculadora cliente-servidor y ayudó a adaptar el registro de estaciones
a Python. También generó los tres módulos de este primer ejemplo, propuso la
lectura completa de los mensajes fragmentados por TCP y preparó la documentación.
Por tanto, el apoyo incluyó generación de código además de consulta conceptual.

El estudiante confirmó haber ejecutado correctamente el ejercicio inicial de
sockets en Java y después eligió Python para la base de Water Management. No se
da por realizada una revisión personal completa de la versión Python: su estudio
y comprobación forman parte del trabajo del estudiante.

## Consultas del estudiante que dieron lugar a esta versión

Las siguientes consultas se recogen literalmente y en orden. El registro cubre
la revisión del material, el aprendizaje de sockets, el anexo y su publicación.

1. «Léete toda la carpeta, con todos los archivos y cuentame que hay»
2. «Entonces cual sería el orden para hacer?»
3. «Vale vamos con el primer punto, porque estoy muy perdido, como se hace? Explica todo paso a paso»
4. «Vale he hecho todo y funciona correctamente, vamos con el anexo»
5. «A ver como esto es la base de Water Management, prefiero hacerla en Python, de ahí que esté el codigo de sockets en python en una carpeta»

La solicitud de publicación pidió subir el trabajo Python a este repositorio,
separar los commits, explicar la práctica en el README y añadir un anexo sobre
el uso consultivo de IA. Se resume aquí para mantener el registro centrado en
el trabajo publicado.

## Conceptos trabajados

- Diferencia entre cliente, servidor, socket, dirección IP y puerto.
- Uso de `bind`, `listen`, `accept` y `connect`.
- Atención de varias conexiones mediante `threading.Thread`.
- Separación entre el mensaje de aplicación y su cabecera de longitud.
- Codificación UTF-8, envío con `sendall` y recepción en varias llamadas a `recv`.
- Validación de campos y cierre de conexiones mediante bloques `with`.
- Diferencia entre el registro por consola del anexo y la persistencia futura.

## Comprobación y responsabilidad

La herramienta realizó pruebas locales de registros correctos, campos vacíos,
operaciones desconocidas, mensajes fragmentados, cabeceras inválidas, caracteres
acentuados y conexiones concurrentes. Estas pruebas ayudan a detectar errores,
pero no demuestran por sí solas el dominio de los conceptos por el estudiante.

Corresponde al estudiante comprender y poder justificar o modificar cualquier
parte entregada, contrastar las decisiones con el enunciado y mantener este
registro actualizado con las nuevas consultas relevantes.
