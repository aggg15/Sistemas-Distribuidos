"""Hola mundo de Kafka para comprobar que el broker funciona.

Terminal 1:  python scripts/prueba_kafka.py consumidor localhost:9092
Terminal 2:  python scripts/prueba_kafka.py productor  localhost:9092
"""

import argparse
import os
import sys
import time

# Permite importar "common" al ejecutar el script desde la raíz del repositorio.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.kafka_utils import crear_consumidor, crear_productor, enviar  # noqa: E402

TOPIC = "wm.prueba"


def productor(servidor):
    kafka = crear_productor(servidor)
    for numero in range(1, 6):
        mensaje = {"tipo": "HOLA", "numero": numero}
        enviar(kafka, TOPIC, mensaje, clave="prueba")
        print(f"Enviado: {mensaje}")
        time.sleep(1)
    kafka.close()


def consumidor(servidor):
    kafka = crear_consumidor(servidor, [TOPIC], grupo="prueba")
    print(f"Esperando mensajes en {TOPIC} (Ctrl+C para salir)...")
    try:
        for registro in kafka:
            print(f"Recibido de partición {registro.partition}, offset {registro.offset}: {registro.value}")
    except KeyboardInterrupt:
        pass
    finally:
        kafka.close()


def main():
    parser = argparse.ArgumentParser(description="Prueba de Kafka")
    parser.add_argument("modo", choices=["productor", "consumidor"])
    parser.add_argument("servidor", help="ip:puerto del broker, por ejemplo localhost:9092")
    args = parser.parse_args()
    productor(args.servidor) if args.modo == "productor" else consumidor(args.servidor)


if __name__ == "__main__":
    main()
