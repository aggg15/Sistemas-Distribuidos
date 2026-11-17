"""Prueba rota a propósito para comprobar que el CI marca el PR en rojo (#24). Se revierte."""
import unittest


class CiRojo(unittest.TestCase):
    def test_falla(self):
        self.fail("Fallo intencionado para verificar el CI")
