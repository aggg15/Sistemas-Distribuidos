"""Aplicación del operario: fichero de peticiones y seguimiento de una solicitud."""

import os
import tempfile
import threading
import unittest
from unittest.mock import patch

try:
    import WM_FO
    from WM_FO import Operario, formatear, leer_fichero
    from common.topics import TOPIC_ESTADO_RED, TOPIC_NOTIFICACIONES
except ImportError:  # kafka-python no instalado
    WM_FO = None


def fichero_temporal(contenido):
    descriptor, ruta = tempfile.mkstemp(suffix=".txt")
    with os.fdopen(descriptor, "w", encoding="utf-8") as fichero:
        fichero.write(contenido)
    return ruta


@unittest.skipIf(WM_FO is None, "Requiere kafka-python")
class PruebasFichero(unittest.TestCase):
    def test_formato_valido_con_comentarios_y_espacios(self):
        ruta = fichero_temporal("# pruebas\nWS-04;20\n\n WS-05 ; 15 \n")
        self.addCleanup(os.remove, ruta)
        self.assertEqual(leer_fichero(ruta), [("WS-04", 20), ("WS-05", 15)])

    def test_lineas_no_validas_indican_el_numero(self):
        for contenido in ("WS-04;20\nWS-05\n", "WS-04;-3\n", "WS-04;veinte\n", "WS#4;5\n"):
            with self.subTest(contenido=contenido):
                ruta = fichero_temporal(contenido)
                self.addCleanup(os.remove, ruta)
                with self.assertRaises(ValueError):
                    leer_fichero(ruta)


@unittest.skipIf(WM_FO is None, "Requiere kafka-python")
class PruebasSeguimiento(unittest.TestCase):
    def setUp(self):
        self.operario = Operario("kafka:9092", "FO1")
        self.enviadas = []

    def central_responde(self, *tipos):
        """Simula a CENTRAL: al recibir la solicitud, contesta con esas notificaciones."""
        def enviar(productor, topic, mensaje, clave=None):
            self.enviadas.append(mensaje)

            def responder():
                for tipo in tipos:
                    self.operario._recibir(TOPIC_NOTIFICACIONES, {
                        "tipo": tipo, "fo_id": mensaje["fo_id"], "ws_id": mensaje["ws_id"],
                        "peticion_id": mensaje["peticion_id"], "texto": tipo,
                    })
            threading.Thread(target=responder).start()
        return patch("WM_FO.enviar", side_effect=enviar)

    def test_riego_completo(self):
        with self.central_responde("PASO", "AUTORIZADO", "TELEMETRIA", "RESUMEN"):
            self.assertTrue(self.operario.solicitar("WS-04", 5))
        self.assertEqual(self.enviadas[0]["origen"], "FO")
        self.assertEqual(self.enviadas[0]["duracion_s"], 5)

    def test_denegado(self):
        with self.central_responde("PASO", "DENEGADO"):
            self.assertFalse(self.operario.solicitar("WS-04", 5))

    def test_central_no_responde(self):
        with self.central_responde(), patch("WM_FO.SILENCIO_MAXIMO_S", 0.1):
            self.assertFalse(self.operario.solicitar("WS-04", 5))

    def test_ignora_notificaciones_de_otros_operarios_y_peticiones(self):
        self.operario._en_curso = "FO1-1"
        self.operario._recibir(TOPIC_NOTIFICACIONES,
                               {"tipo": "RESUMEN", "fo_id": "FO2", "peticion_id": "FO1-1"})
        self.operario._recibir(TOPIC_NOTIFICACIONES,
                               {"tipo": "RESUMEN", "fo_id": "FO1", "peticion_id": "FO1-0"})
        self.assertTrue(self.operario._eventos.empty())

    def test_fichero_espera_entre_peticiones(self):
        ruta = fichero_temporal("WS-04;5\nWS-05;5\nWS-99;5\n")
        self.addCleanup(os.remove, ruta)
        with self.central_responde("RESUMEN"), patch("WM_FO.time.sleep") as dormir:
            self.assertEqual(self.operario.ejecutar_fichero(ruta), [True, True, True])
        self.assertEqual([llamada.args for llamada in dormir.call_args_list], [(4.0,), (4.0,)])
        self.assertEqual([m["ws_id"] for m in self.enviadas], ["WS-04", "WS-05", "WS-99"])

    def test_estado_de_la_red(self):
        estaciones = [{"ws_id": "WS-04", "ubicacion": "Parque", "estado": "DISPONIBLE"}]
        self.operario._recibir(TOPIC_ESTADO_RED, {"tipo": "ESTADO_RED",
                                                 "estaciones": estaciones})
        self.assertEqual(self.operario.estaciones, estaciones)

    def test_formato_de_la_telemetria(self):
        texto = formatear({"tipo": "TELEMETRIA", "ws_id": "WS-04", "caudal_lpm": 12.5,
                           "volumen_l": 1.25, "segundos": 6})
        self.assertIn("12.50 L/min", texto)
        self.assertIn("1.25 L", texto)


if __name__ == "__main__":
    unittest.main()
