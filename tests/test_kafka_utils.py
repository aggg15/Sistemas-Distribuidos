"""La confirmación o el error del broker debe llegar al llamador."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

try:
    from kafka import TopicPartition
    from kafka.errors import KafkaTimeoutError, TopicAuthorizationFailedError
    from common.kafka_utils import (CONFIG_TOPICS, crear_consumidor, enviar, es_atrasado,
                                    fin_inicial)
except ModuleNotFoundError as error:
    if error.name != "kafka":
        raise
    enviar = None


@unittest.skipIf(enviar is None, "Instala requirements.txt para probar las utilidades Kafka")
class PruebasEnvioKafka(unittest.TestCase):
    def setUp(self):
        self.productor = Mock()
        self.futuro = self.productor.send.return_value

    def test_devuelve_metadata_solo_tras_confirmacion(self):
        metadata = SimpleNamespace(topic="wm.prueba", partition=0, offset=7)
        self.futuro.get.return_value = metadata
        self.assertIs(enviar(self.productor, "wm.prueba", {"tipo": "HOLA"}, "WS-01"), metadata)

    def test_error_asincrono_del_broker_no_se_oculta(self):
        self.futuro.get.side_effect = TopicAuthorizationFailedError("Sin permiso")
        with self.assertRaises(TopicAuthorizationFailedError):
            enviar(self.productor, "wm.prueba", {"tipo": "HOLA"})

    def test_timeout_de_confirmacion_se_propaga(self):
        self.futuro.get.side_effect = KafkaTimeoutError("Broker sin respuesta")
        with self.assertRaises(KafkaTimeoutError):
            enviar(self.productor, "wm.prueba", {"tipo": "HOLA"}, timeout=0.1)

    def test_fallo_al_encolar_se_propaga(self):
        self.productor.send.side_effect = KafkaTimeoutError("Sin metadatos")
        with self.assertRaises(KafkaTimeoutError):
            enviar(self.productor, "wm.prueba", {"tipo": "HOLA"})

    def test_limite_tambien_descuenta_la_espera_al_encolar(self):
        with patch("common.kafka_utils.time.monotonic", side_effect=[10, 11]):
            with self.assertRaises(KafkaTimeoutError):
                enviar(self.productor, "wm.prueba", {"tipo": "HOLA"}, timeout=0.5)
        self.futuro.get.assert_not_called()

    def test_sin_metadatos_no_se_da_el_envio_por_confirmado(self):
        self.futuro.get.return_value = None
        with self.assertRaises(KafkaTimeoutError):
            enviar(self.productor, "wm.prueba", {"tipo": "HOLA"})

    def test_timeout_invalido_no_encola_el_mensaje(self):
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                enviar(self.productor, "wm.prueba", {}, timeout=timeout)
        self.productor.send.assert_not_called()


@unittest.skipIf(enviar is None, "Instala requirements.txt para probar las utilidades Kafka")
class PruebasConsumidor(unittest.TestCase):
    """Sin broker: se sustituyen el cliente de administración y el consumidor."""

    def test_crea_los_topics_que_faltan_y_asigna_todas_las_particiones(self):
        with patch("common.kafka_utils.KafkaAdminClient") as admin, \
                patch("common.kafka_utils.KafkaConsumer") as consumidor:
            admin.return_value.list_topics.return_value = ["wm.a"]
            consumidor.return_value.partitions_for_topic.side_effect = lambda t: {0}
            resultado = crear_consumidor("kafka:9092", ["wm.a", "wm.b"], "grupo",
                                         confirmacion_manual=True)
        creados = admin.return_value.create_topics.call_args.args[0]
        self.assertEqual([t.name for t in creados], ["wm.b"])
        # Hora del broker en los mensajes: el topic nuevo se crea así y el que
        # ya existía se ajusta.
        self.assertEqual(creados[0].topic_configs, CONFIG_TOPICS)
        ajustados = admin.return_value.alter_configs.call_args.args[0]
        self.assertEqual([(r.name, r.configs) for r in ajustados], [("wm.a", CONFIG_TOPICS)])
        self.assertFalse(consumidor.call_args.kwargs["enable_auto_commit"])
        asignadas = resultado.assign.call_args.args[0]
        self.assertEqual([(p.topic, p.partition) for p in asignadas],
                         [("wm.a", 0), ("wm.b", 0)])

    def test_salta_lo_acumulado_de_los_topics_solo_nuevos(self):
        # ESTADO_RED acumulado durante días no se lee: se empieza por el final.
        with patch("common.kafka_utils.KafkaAdminClient"), \
                patch("common.kafka_utils.KafkaConsumer") as consumidor:
            consumidor.return_value.partitions_for_topic.side_effect = lambda t: {0}
            resultado = crear_consumidor("kafka:9092", ["wm.ordenes", "wm.estado_red"],
                                         "grupo", solo_nuevos=["wm.estado_red"])
        saltadas = resultado.seek_to_end.call_args.args
        self.assertEqual([(p.topic, p.partition) for p in saltadas], [("wm.estado_red", 0)])

    def test_sin_metadatos_se_cierra_y_avisa(self):
        with patch("common.kafka_utils.KafkaAdminClient"), \
                patch("common.kafka_utils.KafkaConsumer") as consumidor:
            consumidor.return_value.partitions_for_topic.return_value = None
            with self.assertRaises(KafkaTimeoutError):
                crear_consumidor("kafka:9092", ["wm.a"], "grupo")
        consumidor.return_value.close.assert_called_once()

    def test_atrasado_segun_la_posicion_al_conectar_no_segun_el_reloj(self):
        consumidor = Mock()
        consumidor.assignment.return_value = {TopicPartition("wm.a", 0)}
        consumidor.end_offsets.return_value = {TopicPartition("wm.a", 0): 10}
        fin = fin_inicial(consumidor)

        def registro(offset, topic="wm.a"):
            # timestamp absurdo a propósito: no debe influir.
            return SimpleNamespace(topic=topic, partition=0, offset=offset, timestamp=0)

        self.assertTrue(es_atrasado(registro(9), fin))
        self.assertFalse(es_atrasado(registro(10), fin))
        self.assertFalse(es_atrasado(registro(0, topic="wm.otro"), fin))


if __name__ == "__main__":
    unittest.main()
