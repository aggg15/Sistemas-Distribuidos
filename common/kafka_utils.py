"""Productores y consumidores de Kafka que intercambian mensajes JSON."""

from kafka import KafkaConsumer, KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer

# Topics del sistema (ver docs/DISENO.md, apartado 4).
TOPIC_PETICIONES = "wm.peticiones"
TOPIC_ORDENES = "wm.ordenes"
TOPIC_EVENTOS_WS = "wm.eventos_ws"
TOPIC_NOTIFICACIONES = "wm.notificaciones"
TOPIC_ESTADO_RED = "wm.estado_red"


def crear_productor(servidor):
    """servidor: "ip:puerto" del broker, por ejemplo "localhost:9092"."""
    return KafkaProducer(
        bootstrap_servers=servidor,
        # Convierte el diccionario a JSON y la clave a bytes UTF-8.
        value_serializer=JsonSerializer(),
        key_serializer=DefaultSerializer(),
    )


def enviar(productor, topic, mensaje, clave=None):
    # La clave (por ejemplo el ID de la estación) hace que todos sus mensajes
    # vayan a la misma partición y, por tanto, lleguen en orden.
    productor.send(topic, value=mensaje, key=clave)
    # flush() espera a que el broker confirme: poco volumen, preferimos seguridad.
    productor.flush()


def crear_consumidor(servidor, topics, grupo):
    """Cada grupo recibe todos los mensajes; dentro de un grupo se reparten.

    Por eso cada Engine y cada FO usan su propio grupo: así todos reciben
    todo y descartan lo que no va dirigido a ellos.
    """
    return KafkaConsumer(
        *topics,
        bootstrap_servers=servidor,
        group_id=grupo,
        # Al arrancar por primera vez, solo mensajes nuevos: no queremos
        # ejecutar órdenes antiguas que quedaron en el topic.
        auto_offset_reset="latest",
        value_deserializer=JsonSerializer(),
    )
