import time
import random
import json
import os
import numpy as np
from confluent_kafka import Producer

KAFKA_BROKER     = "kafka:9092"
TOPIC_PRINCIPAL  = "consultas_geo"
TOTAL_CONSULTAS  = int(os.getenv("TOTAL_CONSULTAS", 5000))
DISTRIBUCION     = os.getenv("DISTRIBUCION", "uniforme")   
MODO             = os.getenv("MODO", "normal")             
DELAY_MS         = float(os.getenv("DELAY_MS", 0))         

ZONAS = [
    {"id": "Z1", "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    {"id": "Z2", "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    {"id": "Z3", "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    {"id": "Z4", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    {"id": "Z5", "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
]

TIPOS_CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR producer] {err}", flush=True)


def calcular_probabilidades_zipf(n: int, a: float = 1.5) -> np.ndarray:
    probs = np.array([1.0 / (i ** a) for i in range(1, n + 1)])
    return probs / probs.sum()


def elegir_zona(distribucion: str, probs=None):
    if distribucion == "zipf" and probs is not None:
        idx = np.random.choice(len(ZONAS), p=probs)
        return ZONAS[idx]
    return random.choice(ZONAS)


def construir_mensaje(tipo_q: str, zona: dict, i: int) -> dict:
    mensaje = {
        "id_consulta":      f"req_{int(time.time()*1000)}_{i}",
        "tipo":             tipo_q,
        "params": {
            "confidence_min": 0.7,
            "lat_min": zona["lat_min"], "lat_max": zona["lat_max"],
            "lon_min": zona["lon_min"], "lon_max": zona["lon_max"],
        },
        "intentos":          0,
        "timestamp_origen":  time.time(),
    }

    if tipo_q == "q4":
        otras = [z for z in ZONAS if z["id"] != zona["id"]]
        zona_b = random.choice(otras)
        mensaje["params"].update({
            "lat_min_a": zona["lat_min"],   "lat_max_a": zona["lat_max"],
            "lon_min_a": zona["lon_min"],   "lon_max_a": zona["lon_max"],
            "lat_min_b": zona_b["lat_min"], "lat_max_b": zona_b["lat_max"],
            "lon_min_b": zona_b["lon_min"], "lon_max_b": zona_b["lon_max"],
        })
        for k in ["lat_min", "lat_max", "lon_min", "lon_max"]:
            mensaje["params"].pop(k, None)

    if tipo_q == "q5":
        mensaje["params"]["bins"] = 5

    return mensaje


def generar_consultas(total: int, distribucion: str, modo: str):
    print(f"Iniciando generador: {total} consultas | dist={distribucion} | modo={modo}", flush=True)

    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'acks': 1,
        'queue.buffering.max.messages': 100000,
    })

    probs = calcular_probabilidades_zipf(len(ZONAS)) if distribucion == "zipf" else None

    for i in range(total):
        if modo == "spike" and total // 3 <= i <= 2 * total // 3:
            for _ in range(4):                         
                zona   = elegir_zona(distribucion, probs)
                tipo_q = random.choice(TIPOS_CONSULTAS)
                msg    = construir_mensaje(tipo_q, zona, i)
                producer.produce(TOPIC_PRINCIPAL, json.dumps(msg).encode(), callback=delivery_report)
                producer.poll(0)

        zona   = elegir_zona(distribucion, probs)
        tipo_q = random.choice(TIPOS_CONSULTAS)
        msg    = construir_mensaje(tipo_q, zona, i)

        producer.produce(TOPIC_PRINCIPAL, json.dumps(msg).encode(), callback=delivery_report)
        producer.poll(0)

        if DELAY_MS > 0:
            time.sleep(DELAY_MS / 1000.0)

        if i % 100 == 0:
            print(f"[{i}/{total}] mensajes encolados", flush=True)

    producer.flush()
    print("Todas las consultas fueron publicadas en Kafka.", flush=True)


if __name__ == "__main__":
    print("Esperando 15s para que Kafka esté listo...", flush=True)
    time.sleep(15)
    generar_consultas(TOTAL_CONSULTAS, DISTRIBUCION, MODO)