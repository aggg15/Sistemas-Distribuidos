"""Detección de CENTRAL caída a partir de su ESTADO_RED periódico."""

import unittest

from common.latido import LatidoCentral


class PruebasLatido(unittest.TestCase):
    def setUp(self):
        self.ahora = 0.0
        self.latido = LatidoCentral(plazo_s=6, reloj=lambda: self.ahora)

    def test_avisa_una_sola_vez_y_al_volver(self):
        self.ahora = 5
        self.assertFalse(self.latido.revisar())  # aún dentro del plazo
        self.ahora = 7
        self.assertTrue(self.latido.revisar())
        self.assertFalse(self.latido.revisar())  # no se repite el aviso
        self.assertFalse(self.latido.disponible)
        self.assertTrue(self.latido.recibido())  # CENTRAL vuelve
        self.assertFalse(self.latido.recibido())
        self.assertTrue(self.latido.disponible)

    def test_mientras_llegan_latidos_no_avisa(self):
        for segundo in range(2, 30, 2):
            self.ahora = segundo
            self.latido.recibido()
            self.assertFalse(self.latido.revisar())


if __name__ == "__main__":
    unittest.main()
