"""Productores y consumidores de Kafka que intercambian mensajes JSON."""

import math
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaTimeoutError, TopicAlreadyExistsError
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


def crear_topics(servidor, topics):
    """Crea los topics que aún no existan (una partición, sin réplicas: hay un
    solo broker). Así nadie se suscribe a un topic inexistente."""
    admin = KafkaAdminClient(bootstrap_servers=servidor, request_timeout_ms=5000)
    try:
        existentes = set(admin.list_topics())
        nuevos = [NewTopic(topic, num_partitions=1, replication_factor=1)
                  for topic in topics if topic not in existentes]
        if nuevos:
            try:
                admin.create_topics(nuevos)
            except TopicAlreadyExistsError:
                pass  # otro módulo lo creó a la vez
    finally:
        admin.close()


def crear_consumidor(servidor, topics, grupo, confirmacion_manual=False):
    """Consumidor que lee todas las particiones de esos topics.

    Cada módulo (CENTRAL, cada Engine, cada FO) usa su propio grupo, así que
    recibe todos los mensajes y descarta los que no van dirigidos a él. Como es
    el único consumidor de su grupo, se asigna las particiones directamente en
    lugar de suscribirse: no hay que esperar a que Kafka reparta el grupo, lo
    que tras una caída tardaría hasta que expirase la sesión del proceso muerto.
    El grupo sirve para que Kafka guarde hasta dónde se ha leído.

    Con confirmacion_manual, la posición solo se guarda al llamar a commit(),
    después de procesar los mensajes: si el proceso cae antes, se vuelven a leer.
    """
    crear_topics(servidor, topics)
    consumidor = KafkaConsumer(
        bootstrap_servers=servidor,
        group_id=grupo,
        enable_auto_commit=not confirmacion_manual,
        # La primera vez que se usa el grupo, solo mensajes nuevos: no queremos
        # ejecutar órdenes antiguas que quedaron en el topic.
        auto_offset_reset="latest",
        value_deserializer=JsonSerializer(),
    )
    try:
        particiones = []
        for topic in topics:
            numeros = consumidor.partitions_for_topic(topic)
            if not numeros:
                raise KafkaTimeoutError(f"Sin metadatos del topic {topic}")
            particiones += [TopicPartition(topic, numero) for numero in numeros]
        consumidor.assign(particiones)
    except BaseException:
        consumidor.close()
        raise
    return consumidor
