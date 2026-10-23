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
| Pruebas | Elaboración y ejecución de pruebas del protocolo y de la comunicación entre CENTRAL y Monitor. |
| Despliegue | Preparación del Dockerfile, configuración local de Compose y consulta de documentación oficial de Railway. |
| Documentación | Redacción y actualización de las guías de ejecución, diseño y comunicaciones. |

## Verificación realizada

Para la base de comunicaciones se han ejecutado 32 pruebas automatizadas y
comprobaciones con procesos independientes sobre TCP local. Se han verificado
el registro, los latidos, el rechazo de identificadores duplicados, la gestión
de tramas incorrectas, los tiempos de espera y la reconexión tras reiniciar
CENTRAL. También se ha validado la configuración de Docker Compose.

Estas comprobaciones no incluyen la ejecución de las imágenes Docker, el
despliegue en Railway ni la integración de las aplicaciones con Kafka. El detalle
y las limitaciones de la verificación están recogidos en
[la guía de comunicaciones](COMUNICACIONES.md).

## Revisión y responsabilidad técnica

Las propuestas generadas deben contrastarse con los requisitos de la práctica
y revisarse antes de incorporarlas a la entrega. La responsabilidad sobre las
decisiones de diseño, la corrección del sistema y la explicación de su
funcionamiento corresponde a los autores del proyecto.
