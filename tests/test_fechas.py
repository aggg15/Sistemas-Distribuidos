"""La presentación usa Madrid sin modificar los instantes guardados en UTC."""

from pathlib import Path
import json
import subprocess
import sys
import tempfile
import unittest

from common.base_datos import BaseDatos
from common.fechas import fecha_madrid


class PruebasFechas(unittest.TestCase):
    def test_fecha_de_la_captura_en_horario_de_verano(self):
        self.assertEqual(fecha_madrid("2026-10-23T12:59:21+00:00"),
                         "2026-10-23T14:59:21+02:00")

    def test_horario_de_invierno_con_cambio_de_dia(self):
        self.assertEqual(fecha_madrid("2026-01-10T23:30:00+00:00"),
                         "2026-01-11T00:30:00+01:00")

    def test_cambio_de_hora_distingue_los_dos_instantes(self):
        self.assertEqual(fecha_madrid("2026-10-25T00:30:00Z"),
                         "2026-10-25T02:30:00+02:00")
        self.assertEqual(fecha_madrid("2026-10-25T01:30:00Z"),
                         "2026-10-25T02:30:00+01:00")

    def test_fecha_sin_zona_no_se_interpreta_como_hora_del_servidor(self):
        with self.assertRaises(ValueError):
            fecha_madrid("2026-10-23T12:59:21")

    def test_consulta_muestra_madrid_y_conserva_utc_en_sqlite(self):
        with tempfile.TemporaryDirectory() as temporal:
            ruta = Path(temporal) / "wm.db"
            bd = BaseDatos(ruta)
            fila = bd.registrar_estacion("WS-01", "Parque")
            guardada = fila["ultima_conexion"]
            script = Path(__file__).resolve().parent.parent / "scripts" / "gestionar_bd.py"
            salida = subprocess.run([sys.executable, str(script), "--db", str(ruta), "estaciones"],
                                    capture_output=True, text=True, check=True, timeout=4)
            mostrada = json.loads(salida.stdout)[0]["ultima_conexion"]
            self.assertEqual(mostrada, fecha_madrid(guardada))
            self.assertEqual(bd.listar_estaciones()[0]["ultima_conexion"], guardada)
            self.assertTrue(guardada.endswith("+00:00"))


if __name__ == "__main__":
    unittest.main()
