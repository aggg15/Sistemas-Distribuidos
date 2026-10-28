"""Engine (lógica y enlace con el Monitor) y avisos de salud del Monitor a CENTRAL."""

import socket
import tempfile
import threading
import time
import unittest

try:
    from WM_WS_E import Engine, EstacionSimulada
    from common.kafka_utils import TOPIC_EVENTOS_WS, TOPIC_PETICIONES
except ImportError:  # kafka-python no instalado
    Engine = EstacionSimulada = None

from WM_Central import CentralTCP
from WM_WS_M import SaludEngine, mantener_sesion, vigilar_engine


def esperar(condicion, plazo=2.0):
    limite = time.monotonic() + plazo
    while time.monotonic() < limite:
        if condicion():
            return True
        time.sleep(0.01)
    return False


@unittest.skipIf(EstacionSimulada is None, "Requiere kafka-python")
class PruebasEstacion(unittest.TestCase):
    def setUp(self):
        self.publicados = []
        self.kafka_disponible = True

        def publicar(topic, mensaje):
            if self.kafka_disponible:
                self.publicados.append((topic, mensaje))
            return self.kafka_disponible

        self.estacion = EstacionSimulada(publicar, medir_caudal=lambda: 12.0)
        self.assertEqual(self.estacion.responder_monitor("ID#WS-01"), "OK")

    def tipos(self):
        return [mensaje["tipo"] for _, mensaje in self.publicados]

    def iniciar(self, riego_id=1, duracion_s=3):
        self.estacion.procesar_orden({"tipo": "INICIAR_RIEGO", "ws_id": "WS-01",
                                      "riego_id": riego_id, "fo_id": "FO1",
                                      "duracion_s": duracion_s})

    def test_id_y_check(self):
        self.assertEqual(self.estacion.ws_id, "WS-01")
        self.assertEqual(self.estacion.responder_monitor("CHECK"), "OK")
        self.estacion.simular_averia(True)
        self.assertEqual(self.estacion.responder_monitor("CHECK"), "KO")
        self.assertTrue(self.estacion.responder_monitor("ID#WS-99").startswith("KO#"))

    def test_riego_completo_por_tiempo(self):
        self.iniciar(duracion_s=3)
        for _ in range(3):
            self.estacion.avanzar_segundo()
        self.assertEqual(self.tipos(), ["RIEGO_INICIADO", "TELEMETRIA", "TELEMETRIA",
                                        "TELEMETRIA", "RIEGO_FINALIZADO"])
        fin = self.publicados[-1][1]
        self.assertEqual(fin["motivo"], "TIEMPO")
        self.assertEqual(fin["duracion_s"], 3)
        self.assertAlmostEqual(fin["volumen_l"], 0.6)  # 12 L/min durante 3 s
        self.assertIsNone(self.estacion.riego)

    def test_ordenes_de_otra_estacion_se_ignoran(self):
        self.estacion.procesar_orden({"tipo": "INICIAR_RIEGO", "ws_id": "WS-02",
                                      "riego_id": 1, "duracion_s": 3})
        self.estacion.procesar_orden("no es un diccionario")
        self.assertEqual(self.publicados, [])

    def test_averia_corta_el_riego_y_rechaza_nuevos(self):
        self.iniciar()
        self.estacion.simular_averia(True)
        self.assertEqual(self.publicados[-1][1]["motivo"], "FUGA")
        self.iniciar(riego_id=2)
        self.assertEqual(self.publicados[-1][1],
                         {"tipo": "RIEGO_RECHAZADO", "ws_id": "WS-01", "riego_id": 2,
                          "motivo": "FUGA"})

    def test_bloqueo_y_activacion(self):
        self.iniciar()
        self.estacion.procesar_orden({"tipo": "BLOQUEAR", "ws_id": "WS-01"})
        self.assertEqual(self.publicados[-1][1]["motivo"], "BLOQUEO")
        self.iniciar(riego_id=2)
        self.assertEqual(self.publicados[-1][1]["motivo"], "BLOQUEADA")
        self.estacion.procesar_orden({"tipo": "ACTIVAR", "ws_id": "WS-01"})
        self.iniciar(riego_id=3)
        self.assertEqual(self.publicados[-1][1]["tipo"], "RIEGO_INICIADO")

    def test_no_se_inician_dos_riegos_a_la_vez(self):
        self.iniciar(riego_id=1)
        self.iniciar(riego_id=2)
        self.assertEqual(self.publicados[-1][1]["motivo"], "OCUPADA")
        self.assertEqual(self.estacion.riego.riego_id, 1)

    def test_orden_invalida_se_rechaza(self):
        self.estacion.procesar_orden({"tipo": "INICIAR_RIEGO", "ws_id": "WS-01",
                                      "riego_id": 1, "duracion_s": "mucho"})
        self.assertEqual(self.publicados[-1][1]["motivo"], "ORDEN_INVALIDA")

    def test_parar_y_finalizar_sin_riego(self):
        self.iniciar()
        self.estacion.procesar_orden({"tipo": "PARAR", "ws_id": "WS-01", "motivo": "FUGA"})
        self.assertEqual(self.publicados[-1][1]["motivo"], "FUGA")
        self.assertFalse(self.estacion.finalizar("MANUAL"))

    def test_fin_por_tiempo_no_corta_un_riego_posterior(self):
        self.iniciar(riego_id=1)
        self.estacion.finalizar("MANUAL")
        self.iniciar(riego_id=2)
        self.assertFalse(self.estacion.finalizar("TIEMPO", riego_id=1))
        self.assertEqual(self.estacion.riego.riego_id, 2)

    def test_solicitud_manual_va_a_central(self):
        peticion_id = self.estacion.solicitar_riego(20)
        topic, mensaje = self.publicados[-1]
        self.assertEqual(topic, TOPIC_PETICIONES)
        self.assertEqual(mensaje, {"tipo": "SOLICITUD", "peticion_id": peticion_id,
                                   "origen": "WS", "ws_id": "WS-01", "duracion_s": 20})
        self.assertNotEqual(peticion_id, self.estacion.solicitar_riego(20))

    def test_solicitud_sin_kafka_no_se_da_por_enviada(self):
        self.kafka_disponible = False
        with self.assertRaises(ValueError):
            self.estacion.solicitar_riego(20)

    def test_eventos_van_al_topic_de_la_estacion(self):
        self.iniciar()
        self.assertEqual(self.publicados[-1][0], TOPIC_EVENTOS_WS)


@unittest.skipIf(Engine is None, "Requiere kafka-python")
class PruebasEnlaceMonitorEngine(unittest.TestCase):
    """Monitor y Engine reales hablando por un socket TCP local."""

    def setUp(self):
        self.parada_monitor = threading.Event()
        self.confirmado = threading.Event()
        self.salud = SaludEngine()
        servidor = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(servidor.close)
        puerto = servidor.getsockname()[1]
        monitor = threading.Thread(
            target=vigilar_engine,
            args=(servidor, "WS-01", self.salud, self.parada_monitor, self.confirmado),
            kwargs={"intervalo": 0.05, "timeout": 1})
        monitor.start()
        self.addCleanup(monitor.join, 2)
        self.addCleanup(self.parada_monitor.set)

        self.engine = Engine("kafka:9092", "127.0.0.1", puerto, timeout=1, reintento=0.05)
        hilo_engine = threading.Thread(target=self.engine.conectar_monitor)
        hilo_engine.start()
        self.addCleanup(hilo_engine.join, 2)
        self.addCleanup(self.engine.parada.set)

    def test_id_solo_tras_confirmar_registro_y_salud(self):
        time.sleep(0.2)
        self.assertIsNone(self.engine.estacion.ws_id)  # CENTRAL aún no ha confirmado
        self.assertFalse(self.salud.ok)
        self.confirmado.set()
        self.assertTrue(esperar(lambda: self.engine.estacion.ws_id == "WS-01"))
        self.assertTrue(esperar(lambda: self.salud.ok))
        self.engine.estacion.simular_averia(True)
        self.assertTrue(esperar(lambda: not self.salud.ok))
        self.engine.estacion.simular_averia(False)
        self.assertTrue(esperar(lambda: self.salud.ok))

    def test_engine_caido_es_averia(self):
        self.confirmado.set()
        self.assertTrue(esperar(lambda: self.salud.ok))
        self.engine.parada.set()  # el Engine deja de contestar y cierra con EOT
        self.assertTrue(esperar(lambda: not self.salud.ok))


class PruebasAvisosSalud(unittest.TestCase):
    """El Monitor traslada a CENTRAL los cambios de salud del Engine."""

    def test_averia_y_resolucion_llegan_a_central(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        central = CentralTCP(timeout=1, ruta_bd=f"{temporal.name}/wm.db")
        cliente, servidor = socket.socketpair()
        self.addCleanup(cliente.close)
        hilo_central = threading.Thread(target=central.atender, args=(servidor, "prueba"))
        hilo_central.start()

        parada = threading.Event()
        salud = SaludEngine()  # empieza en KO: aún no hay Engine
        hilo_monitor = threading.Thread(
            target=mantener_sesion,
            args=(cliente, "WS-01", "Parque", parada),
            kwargs={"intervalo": 0.3, "timeout": 1, "salud": salud})
        hilo_monitor.start()

        def averia():
            return central.registro.estado().get("WS-01", {}).get("averia")

        self.assertTrue(esperar(lambda: averia() is True))
        salud.actualizar(True)
        self.assertTrue(esperar(lambda: averia() is False))
        salud.actualizar(False)
        self.assertTrue(esperar(lambda: averia() is True))

        parada.set()
        hilo_monitor.join(timeout=2)
        hilo_central.join(timeout=2)
        self.assertFalse(central.registro.estado()["WS-01"]["conectada"])


if __name__ == "__main__":
    unittest.main()
