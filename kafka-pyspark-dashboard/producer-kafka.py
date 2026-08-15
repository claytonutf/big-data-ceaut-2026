#--- Como executar (3):
'''
Selecionar com botão direito o diretório kafka-pyspark-dashboard e clicar em Copy Path

Abrir um novo CMD

digitar:
cd <pressionar ctrl+v> <ENTER>

conda activate ceaut

python producer-kafka.py
'''


import csv
import json
import time

from kafka import KafkaProducer

TOPIC = "input"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    acks="all"
)

CSV_FILE = "logs-maquina.csv"

print("Iniciando Producer...")

with open(CSV_FILE, newline="", encoding="utf-8") as csvfile:

    reader = csv.DictReader(csvfile)

    for row in reader:

        # Conversão dos tipos
        event = {
            "timestamp": row["timestamp"],
            "machine_id": row["machine_id"],
            "machine_type": row["machine_type"],
            "part_id": row["part_id"],
            "operator": row["operator"],
            "cycle_time": float(row["cycle_time"]),
            "temperature": float(row["temperature"]),
            "vibration": float(row["vibration"]),
            "power_consumption": float(row["power_consumption"]),
            "rpm": int(row["rpm"]),
            "tool_number": row["tool_number"],
            "tool_wear": float(row["tool_wear"]),
            "status": row["status"],
            "error_code": row["error_code"] if row["error_code"] else None,
            "production_count": int(row["production_count"])
        }

        future = producer.send(TOPIC, event)

        metadata = future.get(timeout=10)

        print(
            f"[{metadata.offset}] "
            f"{event['machine_id']} -> "
            f"{event['part_id']} "
            f"({event['status']})"
        )

        producer.flush()

        # simula uma máquina produzindo continuamente
        time.sleep(2)

producer.close()

print("\nFim da publicação.")