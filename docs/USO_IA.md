# Uso de herramientas de inteligencia artificial

Durante el desarrollo de Water Management se ha utilizado Codex como herramienta
de apoyo para el análisis técnico, la implementación, la documentación y las
pruebas. La asistencia ha incluido generación y modificación de código.

## Alcance de la asistencia

| Área | Trabajo realizado con apoyo de IA |
| --- | --- |
| Análisis | Revisión del enunciado y evaluación del estado del repositorio frente a los requisitos. |
| Diseño | Propuesta de arquitectura, estados, mensajes, esquema de datos y secuencia de implementación. |
| Comunicaciones | Adaptación del registro por sockets a Python e integración del protocolo STX/ETX/LRC, sesiones persistentes, latidos, tiempos de espera y reconexión. |
| Persistencia | Implementación de SQLite, recuperación de estaciones, administración de operarios y preparación del esquema del historial y presentación de fechas en horario de Madrid. |
| Kafka | Revisión de la confirmación de entrega y propagación de errores y tiempos de espera. |
| Pruebas | Elaboración y ejecución de pruebas del protocolo, comunicaciones, persistencia y errores de envío. |
| Despliegue | Preparación del Dockerfile, configuración local de Compose y consulta de documentación oficial de Railway. |
| Documentación | Redacción y actualización de las guías de ejecución, diseño y comunicaciones. |

## Verificación realizada

Se han ejecutado 56 pruebas automatizadas sobre protocolo, sesiones TCP,
persistencia SQLite y errores de envío Kafka. Las comprobaciones incluyen
recuperación de estaciones al reiniciar, altas concurrentes, rechazo de
identificadores duplicados y fallos de escritura sin confirmación del registro.
También se verifica la conversión a Madrid en verano e invierno sin modificar
los instantes UTC almacenados.

Se ha construido la imagen Docker y verificado que estaciones y operarios
sobreviven a recrear CENTRAL con un volumen persistente. También se ha comprobado
el envío confirmado y la recepción ordenada de mensajes JSON contra Kafka local.
Las pruebas utilizaron recursos temporales independientes de los datos normales.

El despliegue en Railway y la integración del ciclo completo de riego siguen
pendientes. Los procedimientos y límites de verificación se describen en las
guías de [comunicaciones](COMUNICACIONES.md) y [persistencia](PERSISTENCIA.md).

## Revisión y responsabilidad técnica

Las propuestas generadas deben contrastarse con los requisitos de la práctica
y revisarse antes de incorporarlas a la entrega. La responsabilidad sobre las
decisiones de diseño, la corrección del sistema y la explicación de su
funcionamiento corresponde a los autores del proyecto.
