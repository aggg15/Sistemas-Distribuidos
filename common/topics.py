"""Topics de Kafka del sistema (docs/DISENO.md, apartado 4).

Están aparte de kafka_utils para poder usarlos sin tener kafka-python instalado.
"""

TOPIC_PETICIONES = "wm.peticiones"          # FO o WS → CENTRAL
TOPIC_ORDENES = "wm.ordenes"                # CENTRAL → Engine
TOPIC_EVENTOS_WS = "wm.eventos_ws"          # Engine → CENTRAL
TOPIC_NOTIFICACIONES = "wm.notificaciones"  # CENTRAL → FO
TOPIC_ESTADO_RED = "wm.estado_red"          # CENTRAL → FO
