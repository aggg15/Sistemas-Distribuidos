"""Panel remoto (ECS): debe comportarse igual que el panel de CENTRAL."""

import json
import socket
import threading
import unittest
import urllib.error
import urllib.request

from panel_remoto import crear_panel_remoto
# Se importa el módulo y no la clase: si no, unittest ejecutaría aquí también
# las pruebas del panel normal.
from tests import test_panel


def arrancar(servidor, prueba):
    threading.Thread(target=servidor.serve_forever, daemon=True).start()
    prueba.addCleanup(servidor.server_close)
    prueba.addCleanup(servidor.shutdown)
    return f"http://127.0.0.1:{servidor.server_address[1]}"


class PruebasPanelRemoto(test_panel.PruebasPanel):
    """Repite todas las pruebas del panel, pero pasando por el panel remoto."""

    def setUp(self):
        super().setUp()
        self.base = arrancar(crear_panel_remoto(self.base, host="127.0.0.1", puerto=0), self)


class PruebasCentralCaida(unittest.TestCase):
    def setUp(self):
        # Un puerto libre en el que no escucha nadie: CENTRAL "caída".
        with socket.socket() as libre:
            libre.bind(("127.0.0.1", 0))
            puerto_libre = libre.getsockname()[1]
        self.base = arrancar(crear_panel_remoto(f"http://127.0.0.1:{puerto_libre}",
                                                host="127.0.0.1", puerto=0), self)

    def test_la_pagina_se_sirve_igualmente(self):
        with urllib.request.urlopen(self.base + "/", timeout=2) as respuesta:
            self.assertEqual(respuesta.status, 200)

    def test_estado_da_502(self):
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(self.base + "/api/estado", timeout=7)
        with error.exception:
            self.assertEqual(error.exception.code, 502)
            self.assertIn("Sin conexión con WM_Central",
                          json.loads(error.exception.read())["error"])


if __name__ == "__main__":
    unittest.main()
