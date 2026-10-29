"""Las pruebas importan los módulos por su nombre (from WM_FO import ...).

Se añaden al path la raíz (para common) y la carpeta de cada aplicación.
"""

from pathlib import Path
import sys

RAIZ = Path(__file__).resolve().parent.parent
for carpeta in (RAIZ, RAIZ / "WM_Central", RAIZ / "WM_WS", RAIZ / "WM_FO"):
    sys.path.insert(0, str(carpeta))
