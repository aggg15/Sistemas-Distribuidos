"""Conversión de fechas persistidas a la zona de presentación de la aplicación."""

from datetime import datetime
from zoneinfo import ZoneInfo


def fecha_madrid(texto):
    """Conserva el instante y aplica el horario de verano/invierno de Madrid."""
    fecha = datetime.fromisoformat(texto.replace("Z", "+00:00"))
    if fecha.tzinfo is None:
        raise ValueError("La fecha guardada no tiene zona horaria")
    return fecha.astimezone(ZoneInfo("Europe/Madrid")).isoformat(timespec="seconds")
