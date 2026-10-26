"""Persistencia, concurrencia e integridad sobre ficheros SQLite temporales."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from WM_Central import CentralTCP, RegistroConexiones, SesionMonitor
from common.base_datos import BaseDatos


class PruebasPersistencia(unittest.TestCase):
    def setUp(self):
        temporal = tempfile.TemporaryDirectory()
        self.addCleanup(temporal.cleanup)
        self.ruta = Path(temporal.name) / "datos" / "wm.db"
        self.bd = BaseDatos(self.ruta)

    def test_tablas_vacias_en_primera_ejecucion(self):
        self.assertEqual(self.bd.listar_estaciones(), [])
        self.assertEqual(self.bd.listar_operarios(), [])
        self.assertEqual(self.bd.listar_riegos(), [])

    def test_estacion_y_operario_sobreviven_a_nueva_instancia(self):
        self.bd.registrar_estacion("WS-01", "Jardín Botánico")
        self.bd.guardar_operario("FO-01", "Operario 1")
        recuperada = BaseDatos(self.ruta)
        self.assertEqual(recuperada.listar_estaciones()[0]["ubicacion"], "Jardín Botánico")
        self.assertTrue(recuperada.operario_existe("FO-01"))
        self.assertFalse(recuperada.operario_existe("FO-99"))
        self.assertEqual(recuperada.listar_operarios(), [{"id": "FO-01", "nombre": "Operario 1"}])

    def test_reconexion_actualiza_sin_duplicar_ni_perder_bloqueo(self):
        self.bd.registrar_estacion("WS-01", "Parque")
        # Simula el bloqueo persistido por una versión del control de estaciones.
        with closing(sqlite3.connect(self.ruta)) as conexion:
            with conexion:
                conexion.execute("UPDATE estaciones SET bloqueada=1 WHERE id='WS-01'")
        self.bd.registrar_estacion("WS-01", "Jardín")
        filas = self.bd.listar_estaciones()
        self.assertEqual(len(filas), 1)
        self.assertEqual(filas[0]["ubicacion"], "Jardín")
        self.assertEqual(filas[0]["bloqueada"], 1)
        self.assertIsNotNone(filas[0]["ultima_conexion"])

    def test_operario_existente_actualiza_el_nombre(self):
        self.bd.guardar_operario("FO-01", "Operario 1")
        self.bd.guardar_operario("FO-01", "Operario actualizado")
        self.assertEqual(self.bd.listar_operarios(),
                         [{"id": "FO-01", "nombre": "Operario actualizado"}])

    def test_sql_parametrizado_con_identificador_que_contiene_comillas(self):
        ws_id = "WS'); DROP TABLE estaciones;--"
        self.bd.registrar_estacion(ws_id, "Parque")
        self.bd.registrar_estacion("WS-02", "Otro parque")
        self.assertEqual(len(self.bd.listar_estaciones()), 2)
        self.assertIn(ws_id, [fila["id"] for fila in self.bd.listar_estaciones()])

    def test_altas_desde_varios_hilos(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda n: self.bd.registrar_estacion(f"WS-{n:02}", f"Parque {n}"),
                          range(18)))
        self.assertEqual(len(self.bd.listar_estaciones()), 18)

    def test_reinicio_recupera_datos_pero_no_sesion_ni_salud(self):
        primera = RegistroConexiones(self.bd)
        sesion = SesionMonitor(primera)
        self.assertEqual(sesion.procesar("REGISTRO#WS-01#Parque"), "OK#WS-01")
        self.assertEqual(sesion.procesar("AVERIA#WS-01"), "OK")
        self.assertTrue(primera.estado()["WS-01"]["conectada"])
        central = CentralTCP(ruta_bd=self.ruta)
        estado = central.registro.estado()["WS-01"]
        self.assertFalse(estado["conectada"])
        self.assertFalse(estado["averia"])
        self.assertEqual(estado["estado"], "DESCONECTADA")
        self.assertEqual(estado["ubicacion"], "Parque")
        nuevo = SesionMonitor(central.registro)
        self.assertEqual(nuevo.procesar("REGISTRO#WS-01#Jardín"), "OK#WS-01")
        self.assertEqual(len(self.bd.listar_estaciones()), 1)

    def test_registro_duplicado_no_modifica_la_fila_persistente(self):
        registro = RegistroConexiones(self.bd)
        primera = SesionMonitor(registro)
        primera.procesar("REGISTRO#WS-01#Parque")
        antes = self.bd.listar_estaciones()
        segunda = SesionMonitor(registro)
        self.assertEqual(segunda.procesar("REGISTRO#WS-01#Otro parque"), "KO#ID_EN_USO")
        self.assertEqual(self.bd.listar_estaciones(), antes)
        self.assertEqual(primera.procesar("PING#WS-01"), "OK")

    def test_error_al_guardar_no_confirma_ni_crea_una_sesion(self):
        registro = RegistroConexiones(self.bd)
        sesion = SesionMonitor(registro)
        with patch.object(self.bd, "registrar_estacion", side_effect=sqlite3.OperationalError("locked")), \
                self.assertLogs("WM_Central", level="ERROR"):
            self.assertEqual(sesion.procesar("REGISTRO#WS-01#Parque"), "KO#ERROR_BD")
        self.assertIsNone(sesion.ws_id)
        self.assertEqual(registro.estado(), {})
        self.assertEqual(self.bd.listar_estaciones(), [])

    def test_datos_invalidos_no_se_guardan(self):
        for ws_id, ubicacion in (("", "Parque"), ("WS#01", "Parque"), ("WS-01", " ")):
            with self.subTest(ws_id=ws_id, ubicacion=ubicacion), self.assertRaises(ValueError):
                self.bd.registrar_estacion(ws_id, ubicacion)
        with self.assertRaises(ValueError):
            self.bd.guardar_operario("FO-01", "")
        self.assertEqual(self.bd.listar_estaciones(), [])
        self.assertEqual(self.bd.listar_operarios(), [])

    def test_historial_exige_estacion_y_operario_existentes(self):
        # Comprueba las restricciones que utilizará el futuro ciclo de riego.
        with self.bd._conexion() as conexion:
            with self.assertRaises(sqlite3.IntegrityError):
                conexion.execute("INSERT INTO riegos (ws_id, origen) VALUES ('WS-99', 'WS')")
        self.bd.registrar_estacion("WS-01", "Parque")
        with self.bd._conexion() as conexion:
            with self.assertRaises(sqlite3.IntegrityError):
                conexion.execute("INSERT INTO riegos (ws_id, fo_id, origen) VALUES ('WS-01', 'FO-99', 'FO')")
        self.assertEqual(self.bd.listar_riegos(), [])

    def test_script_admin_escribe_y_consulta_el_mismo_fichero(self):
        script = Path(__file__).resolve().parent.parent / "scripts" / "gestionar_bd.py"
        def ejecutar(*args):
            return subprocess.run([sys.executable, str(script), "--db", str(self.ruta), *args],
                                  capture_output=True, text=True, timeout=4)
        self.assertEqual(ejecutar("operario", "FO-01", "Operario 1").returncode, 0)
        self.assertIn('"id": "FO-01"', ejecutar("operarios").stdout)
        self.assertEqual(ejecutar("operario", "", "Nombre").returncode, 1)
        self.assertTrue(self.bd.operario_existe("FO-01"))


if __name__ == "__main__":
    unittest.main()
