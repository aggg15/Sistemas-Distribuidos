"""Ciclo de riego completo: CENTRAL (gestor) y Engine unidos por un bus en memoria.

El bus sustituye a Kafka: cada mensaje publicado se entrega en el momento al
módulo que escucha ese topic. Así se prueba el recorrido sin broker.
"""

import socket
import tempfile
import threading
import time
import unittest

from common.base_datos import BaseDatos
from common.protocolo import Canal
from common.topics import (TOPIC_ESTADO_RED, TOPIC_EVENTOS_WS, TOPIC_NOTIFICACIONES,
                           TOPIC_ORDENES)
from gestor_riegos import GestorRiegos
from WM_Central import CentralTCP, RegistroConexiones

try:
    from WM_WS_E import EstacionSimulada
except ImportError:  # kafka-python no instalado
    EstacionSimulada = None


class Bus:
    def __init__(self):
        self.destinos = {}
        self.mensajes = []
        self.caidos = set()  # topics en los que "Kafka" falla

    def publicar(self, topic, mensaje, clave=None):
        if topic in self.caidos:
            return False
        self.mensajes.append((topic, mensaje))
        if topic in self.destinos:
            self.destinos[topic](mensaje)
        return True

    def de(self, topic):
        return [mensaje for t, mensaje in self.mensajes if t == topic]


def solicitud(ws_id="WS-01", duracion_s=3, fo_id="FO1", peticion_id="P1"):
    return {"tipo": "SOLICITUD", "peticion_id": peticion_id, "origen": "FO",
            "fo_id": fo_id, "ws_id": ws_id, "duracion_s": duracion_s}


@unittest.skipIf(EstacionSimulada is None, "Requiere kafka-python")
class PruebasCicloDeRiego(unittest.TestCase):
    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.bd = BaseDatos(f"{temporal.name}/wm.db")
        self.bd.guardar_operario("FO1", "Ana")
        self.registro = RegistroConexiones(self.bd)
        self.registro.registrar("WS-01", "Parque", sesion=object())
        self.registro.registrar("WS-02", "Jardín", sesion=object())

        self.ahora = 0.0
        self.bus = Bus()
        self.gestor = GestorRiegos(self.registro, self.bd, self.bus.publicar,
                                   reloj=lambda: self.ahora)
        self.engine = EstacionSimulada(self.bus.publicar, medir_caudal=lambda: 12.0)
        self.engine.responder_monitor("ID#WS-01")
        self.bus.destinos[TOPIC_ORDENES] = self.engine.procesar_orden
        self.bus.destinos[TOPIC_EVENTOS_WS] = self.gestor.procesar_evento

    def notificaciones(self):
        return [m["tipo"] for m in self.bus.de(TOPIC_NOTIFICACIONES)]

    def estado(self, ws_id="WS-01"):
        return self.registro.estado()[ws_id]["estado"]

    def test_riego_completo_solicitado_por_un_operario(self):
        self.gestor.procesar_peticion(solicitud(duracion_s=3))
        self.assertEqual(self.notificaciones(), ["PASO", "PASO", "PASO", "AUTORIZADO"])
        self.assertEqual(self.estado(), "REGANDO")
        for _ in range(3):
            self.engine.avanzar_segundo()
        self.assertEqual(self.notificaciones()[-4:],
                         ["TELEMETRIA", "TELEMETRIA", "TELEMETRIA", "RESUMEN"])
        resumen = self.bus.de(TOPIC_NOTIFICACIONES)[-1]
        self.assertEqual((resumen["motivo"], resumen["duracion_s"], resumen["peticion_id"]),
                         ("TIEMPO", 3, "P1"))
        self.assertAlmostEqual(resumen["volumen_l"], 0.6)
        self.assertEqual(self.estado(), "DISPONIBLE")
        riego = self.bd.listar_riegos()[0]
        self.assertEqual((riego["fo_id"], riego["origen"], riego["motivo_fin"]),
                         ("FO1", "FO", "TIEMPO"))
        self.assertIsNotNone(riego["inicio"])

    def test_telemetria_visible_en_central(self):
        self.gestor.procesar_peticion(solicitud(duracion_s=5))
        self.engine.avanzar_segundo()
        riego = self.registro.estado()["WS-01"]["riego"]
        self.assertEqual((riego["fo_id"], riego["segundos"]), ("FO1", 1))
        self.assertAlmostEqual(riego["volumen_l"], 0.2)

    def test_operario_no_registrado(self):
        self.gestor.procesar_peticion(solicitud(fo_id="FO9"))
        # El aviso se envía al operario aunque no esté registrado: así sabe por qué.
        self.assertEqual(self.notificaciones(), ["PASO", "DENEGADO"])
        self.assertEqual(self.bd.listar_riegos(), [])

    def test_estacion_no_disponible(self):
        self.registro.avisar_averia("WS-01", self.registro._estaciones["WS-01"].sesion, True)
        self.gestor.procesar_peticion(solicitud())
        self.assertIn("FUGA", self.bus.de(TOPIC_NOTIFICACIONES)[-1]["texto"])
        self.gestor.procesar_peticion(solicitud(ws_id="WS-99"))
        self.assertEqual(self.bus.de(TOPIC_NOTIFICACIONES)[-1]["texto"], "La estación no existe")
        self.assertEqual(self.bus.de(TOPIC_ORDENES), [])

    def test_no_dos_riegos_a_la_vez(self):
        self.gestor.procesar_peticion(solicitud(peticion_id="P1"))
        self.gestor.procesar_peticion(solicitud(peticion_id="P2"))
        denegado = self.bus.de(TOPIC_NOTIFICACIONES)[-1]
        self.assertEqual((denegado["tipo"], denegado["peticion_id"]), ("DENEGADO", "P2"))
        self.assertEqual(self.engine.riego.riego_id, 1)

    def test_averia_durante_el_riego(self):
        self.gestor.procesar_peticion(solicitud(duracion_s=10))
        self.engine.avanzar_segundo()
        self.gestor.interrumpir("WS-01", "FUGA")
        resumen = self.bus.de(TOPIC_NOTIFICACIONES)[-1]
        self.assertEqual((resumen["tipo"], resumen["motivo"]), ("RESUMEN", "FUGA"))
        self.assertIsNone(self.engine.riego)  # el Engine recibió PARAR
        self.assertEqual(self.bd.listar_riegos()[0]["motivo_fin"], "FUGA")
        # El RIEGO_FINALIZADO que manda el Engine al parar no genera otro resumen.
        self.assertEqual(self.notificaciones().count("RESUMEN"), 1)

    def test_engine_que_no_contesta(self):
        del self.bus.destinos[TOPIC_ORDENES]  # la orden se envía pero nadie la atiende
        self.gestor.procesar_peticion(solicitud())
        self.gestor.revisar_plazos()
        self.assertEqual(self.notificaciones()[-1], "PASO")  # aún dentro del plazo
        self.ahora += 6
        self.gestor.revisar_plazos()
        self.assertEqual(self.notificaciones()[-1], "DENEGADO")
        self.assertEqual(self.bus.de(TOPIC_ORDENES)[-1]["tipo"], "PARAR")
        self.assertEqual(self.bd.listar_riegos(), [])
        self.assertEqual(self.estado(), "DISPONIBLE")

    def test_engine_rechaza_el_riego(self):
        self.engine.simular_averia(True)  # CENTRAL aún no lo sabe
        self.gestor.procesar_peticion(solicitud())
        denegado = self.bus.de(TOPIC_NOTIFICACIONES)[-1]
        self.assertEqual(denegado["tipo"], "DENEGADO")
        self.assertIn("FUGA", denegado["texto"])
        self.assertEqual(self.bd.listar_riegos(), [])

    def test_kafka_caido_al_enviar_la_orden(self):
        self.bus.caidos.add(TOPIC_ORDENES)
        self.gestor.procesar_peticion(solicitud())
        self.assertEqual(self.notificaciones()[-1], "DENEGADO")
        self.assertEqual(self.estado(), "DISPONIBLE")

    def test_solicitud_caducada(self):
        self.gestor.procesar_peticion(solicitud(), antiguedad_s=60)
        self.assertEqual(self.notificaciones(), ["PASO", "DENEGADO"])

    def test_bloquear_todas_y_activar(self):
        self.gestor.procesar_peticion(solicitud())
        self.gestor.ordenar("BLOQUEAR")
        self.assertEqual(self.bus.de(TOPIC_NOTIFICACIONES)[-1]["motivo"], "BLOQUEO")
        self.assertEqual((self.estado("WS-01"), self.estado("WS-02")),
                         ("FUERA_SERVICIO", "FUERA_SERVICIO"))
        self.assertTrue(all(fila["bloqueada"] for fila in self.bd.listar_estaciones()))
        self.assertTrue(self.engine.bloqueada)
        self.gestor.ordenar("ACTIVAR", "WS-01")
        self.assertEqual((self.estado("WS-01"), self.estado("WS-02")),
                         ("DISPONIBLE", "FUERA_SERVICIO"))

    def test_riego_iniciado_desde_central(self):
        self.gestor.ordenar("INICIAR", "WS-01", duracion_s=2)
        self.assertEqual(self.estado(), "REGANDO")
        self.assertEqual(self.bus.de(TOPIC_NOTIFICACIONES), [])  # no hay operario
        self.assertEqual(self.bd.listar_riegos()[0]["origen"], "CENTRAL")

    def test_riego_manual_desde_la_estacion(self):
        self.gestor.procesar_peticion({"tipo": "SOLICITUD", "peticion_id": "WS-01-1",
                                       "origen": "WS", "ws_id": "WS-01", "duracion_s": 2})
        self.assertEqual(self.estado(), "REGANDO")
        self.assertEqual(self.bd.listar_riegos()[0]["origen"], "WS")

    def test_estado_de_la_red(self):
        self.gestor.procesar_peticion(solicitud())
        self.gestor.publicar_estado_red()
        red = self.bus.de(TOPIC_ESTADO_RED)[-1]["estaciones"]
        self.assertEqual([(e["ws_id"], e["estado"]) for e in red],
                         [("WS-01", "REGANDO"), ("WS-02", "DISPONIBLE")])

    def test_mensajes_raros_no_rompen_nada(self):
        self.gestor.procesar_peticion({"tipo": "OTRA"})
        self.gestor.procesar_peticion(solicitud(duracion_s="x"))
        self.gestor.procesar_evento({"tipo": "TELEMETRIA", "ws_id": "WS-01", "riego_id": 99})
        self.gestor.procesar_evento(None)
        self.assertEqual(self.estado(), "DISPONIBLE")


@unittest.skipIf(EstacionSimulada is None, "Requiere kafka-python")
class PruebasAveriaDesdeElMonitor(unittest.TestCase):
    """Un AVERIA que llega por el socket del Monitor corta el riego en curso."""

    def test_averia_por_socket_corta_el_riego(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        central = CentralTCP(timeout=1, ruta_bd=f"{temporal.name}/wm.db")
        bus = Bus()
        central.gestor.publicar = bus.publicar
        engine = EstacionSimulada(bus.publicar, medir_caudal=lambda: 12.0)
        engine.responder_monitor("ID#WS-01")
        bus.destinos[TOPIC_ORDENES] = engine.procesar_orden
        bus.destinos[TOPIC_EVENTOS_WS] = central.gestor.procesar_evento

        cliente, servidor = socket.socketpair()
        self.addCleanup(cliente.close)
        hilo = threading.Thread(target=central.atender, args=(servidor, "prueba"))
        hilo.start()
        canal = Canal(cliente, timeout=1)
        canal.saludar()
        canal.peticion("REGISTRO#WS-01#Parque")

        central.gestor.ordenar("INICIAR", "WS-01", duracion_s=10)
        self.assertIsNotNone(engine.riego)
        self.assertEqual(canal.peticion("AVERIA#WS-01"), "OK")
        # CENTRAL corta el riego en otro hilo: se espera a que quede cerrado en la BD.
        limite = time.monotonic() + 2
        while (central.bd.listar_riegos()[0]["motivo_fin"] is None
               and time.monotonic() < limite):
            time.sleep(0.01)
        self.assertIsNone(engine.riego)
        self.assertEqual(central.registro.estado()["WS-01"]["estado"], "FUGA")
        self.assertEqual(central.bd.listar_riegos()[0]["motivo_fin"], "FUGA")

        canal.cerrar()
        hilo.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
