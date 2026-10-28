"""Panel web de CENTRAL: estado en JSON, página y órdenes por HTTP."""

import json
import logging
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from WM_Central import CentralTCP
from panel_web import RegistroEventos, crear_panel, validar_orden


class PruebasPanel(unittest.TestCase):
    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.central = CentralTCP(timeout=1, ruta_bd=f"{temporal.name}/wm.db")
        self.central.registro.registrar("WS-01", "Parque <b>Norte</b>", sesion=object())
        self.central.registro.registrar("WS-02", "Jardín", sesion=object())
        self.publicados = []
        self.central.gestor.publicar = (
            lambda topic, mensaje, clave: self.publicados.append(mensaje) or True)

        self.eventos = RegistroEventos()
        log = logging.getLogger("WM_Central")
        # Sin basicConfig el nivel efectivo es WARNING y los INFO no llegarían al panel.
        nivel_anterior = log.level
        log.setLevel(logging.INFO)
        log.addHandler(self.eventos)
        self.addCleanup(log.setLevel, nivel_anterior)
        self.addCleanup(log.removeHandler, self.eventos)
        self.panel = crear_panel(self.central, self.eventos, host="127.0.0.1", puerto=0)
        threading.Thread(target=self.panel.serve_forever, daemon=True).start()
        self.addCleanup(self.panel.server_close)
        self.addCleanup(self.panel.shutdown)
        self.base = f"http://127.0.0.1:{self.panel.server_address[1]}"

    def get(self, ruta):
        with urllib.request.urlopen(self.base + ruta, timeout=2) as respuesta:
            return respuesta.status, respuesta.headers["Content-Type"], respuesta.read()

    def post(self, cuerpo):
        datos = cuerpo if isinstance(cuerpo, bytes) else json.dumps(cuerpo).encode()
        peticion = urllib.request.Request(self.base + "/api/orden", data=datos, method="POST",
                                          headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(peticion, timeout=2) as respuesta:
                return respuesta.status, json.loads(respuesta.read())
        except urllib.error.HTTPError as error:
            with error:
                return error.code, json.loads(error.read())

    def test_pagina(self):
        codigo, tipo, cuerpo = self.get("/")
        self.assertEqual(codigo, 200)
        self.assertIn("text/html", tipo)
        self.assertIn(b"/api/estado", cuerpo)

    def test_estado_en_json(self):
        codigo, _, cuerpo = self.get("/api/estado")
        datos = json.loads(cuerpo)
        self.assertEqual(codigo, 200)
        self.assertEqual([(e["ws_id"], e["estado"]) for e in datos["estaciones"]],
                         [("WS-01", "DISPONIBLE"), ("WS-02", "DISPONIBLE")])
        # El texto llega tal cual: es la página la que lo muestra sin interpretar HTML.
        self.assertEqual(datos["estaciones"][0]["ubicacion"], "Parque <b>Norte</b>")
        self.assertFalse(datos["kafka"])

    def test_bloquear_y_activar_desde_el_panel(self):
        self.assertEqual(self.post({"accion": "BLOQUEAR", "ws_id": "WS-01"}), (200, {"ok": True}))
        estado = self.central.registro.estado()
        self.assertEqual((estado["WS-01"]["estado"], estado["WS-02"]["estado"]),
                         ("FUERA_SERVICIO", "DISPONIBLE"))
        self.assertEqual(self.post({"accion": "ACTIVAR", "ws_id": None})[0], 200)
        self.assertEqual(self.central.registro.estado()["WS-01"]["estado"], "DISPONIBLE")
        textos = [e["texto"] for e in json.loads(self.get("/api/estado")[2])["eventos"]]
        self.assertTrue(any("Panel: orden BLOQUEAR sobre WS-01" in t for t in textos))

    def test_iniciar_riego_desde_el_panel(self):
        self.assertEqual(self.post({"accion": "INICIAR", "ws_id": "WS-02", "duracion_s": 20})[0],
                         200)
        orden = self.publicados[-1]
        self.assertEqual((orden["tipo"], orden["ws_id"], orden["duracion_s"]),
                         ("INICIAR_RIEGO", "WS-02", 20))

    def test_ordenes_no_validas(self):
        for cuerpo in ({"accion": "BORRAR"}, {"accion": "BLOQUEAR", "ws_id": "WS-99"},
                       {"accion": "INICIAR", "ws_id": "WS-01"},
                       {"accion": "INICIAR", "ws_id": "WS-01", "duracion_s": 0},
                       {"accion": "INICIAR", "ws_id": "WS-01", "duracion_s": True},
                       [1, 2], b"no es json"):
            with self.subTest(cuerpo=cuerpo):
                codigo, datos = self.post(cuerpo)
                self.assertEqual(codigo, 400)
                self.assertIn("error", datos)
        self.assertEqual(self.publicados, [])

    def test_ruta_desconocida(self):
        with self.assertRaises(urllib.error.HTTPError) as error:
            self.get("/otra")
        with error.exception:
            self.assertEqual(error.exception.code, 404)


class PruebasValidacion(unittest.TestCase):
    def test_orden_sobre_todas(self):
        self.assertEqual(validar_orden({"accion": "INICIAR", "ws_id": None, "duracion_s": 30},
                                       ["WS-01"]), ("INICIAR", None, 30))


if __name__ == "__main__":
    unittest.main()
