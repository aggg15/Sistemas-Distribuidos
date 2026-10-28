"""Productores y consumidores de Kafka que intercambian mensajes JSON."""

import math
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaTimeoutError
from kafka.serializer import DefaultSerializer, JsonSerializer

# Se reexportan para que quien ya importaba los topics desde aquí siga funcionando.
from common.topics import (  # noqa: F401
    TOPIC_ESTADO_RED, TOPIC_EVENTOS_WS, TOPIC_NOTIFICACIONES, TOPIC_ORDENES,
    TOPIC_PETICIONES,
)


def crear_productor(servidor):
    """servidor: "ip:puerto" del broker, por ejemplo "localhost:9092"."""
    return KafkaProducer(
        bootstrap_servers=servidor,
        acks="all",
        max_block_ms=10000,
        request_timeout_ms=5000,
        delivery_timeout_ms=10000,
        # Convierte el diccionario a JSON y la clave a bytes UTF-8.
        value_serializer=JsonSerializer(),
        key_serializer=DefaultSerializer(),
    )


def enviar(productor, topic, mensaje, clave=None, timeout=10.0):
    """Devuelve los metadatos confirmados o propaga el error de entrega.

    Un timeout deja el resultado incierto: el mensaje podría llegar después.
    No se reenvía automáticamente desde la aplicación.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("El tiempo de espera debe ser positivo y finito")
    # La clave (por ejemplo el ID de la estación) hace que todos sus mensajes
    # vayan a la misma partición y, por tanto, lleguen en orden.
    limite = time.monotonic() + timeout
    resultado = productor.send(topic, value=mensaje, key=clave)
    restante = limite - time.monotonic()
    if restante <= 0:
        raise KafkaTimeoutError("Tiempo agotado antes de confirmar la entrega")
    metadata = resultado.get(timeout=restante)
    if metadata is None:
        raise KafkaTimeoutError("No se recibió confirmación de Kafka")
    return metadata


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
