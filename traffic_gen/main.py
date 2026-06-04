import time
import random
import json
import numpy as np
from confluent_kafka import Producer

KAFKA_BROKER = "kafka:9092"
TOPIC_PRINCIPAL = "consultas_geo"
TOTAL_CONSULTAS = 5000 

ZONAS = [
    {"id": "Z1", "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    {"id": "Z2", "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    {"id": "Z3", "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    {"id": "Z4", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    {"id": "Z5", "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760}
]

TIPOS_CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]

def delivery_report(err, msg):
    """Callback que se ejecuta cuando Kafka confirma que recibió el mensaje"""
    if err is not None:
        print(f" Error al enviar mensaje: {err}", flush=True)

def generar_consultas(tipo_distribucion="uniforme"):
    
    print(f" {TOTAL_CONSULTAS} consultas ({tipo_distribucion.upper()})", flush=True)
    
    # Iniciamos el Productor de Kafka
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    
    if tipo_distribucion == "zipf":
        a = 1.5
        probabilidades = [1.0 / (i**a) for i in range(1, len(ZONAS) + 1)]
        probabilidades /= np.sum(probabilidades)

    for i in range(TOTAL_CONSULTAS):
        if tipo_distribucion == "uniforme":
            zona = random.choice(ZONAS)
        else:
            zona = np.random.choice(ZONAS, p=probabilidades)
        
        tipo_q = random.choice(TIPOS_CONSULTAS)
        
        mensaje = {
            "id_consulta": f"req_{int(time.time()*1000)}_{i}",
            "tipo": tipo_q,
            "params": {
                "confidence_min": 0.7, 
                "lat_min": zona["lat_min"], "lat_max": zona["lat_max"], 
                "lon_min": zona["lon_min"], "lon_max": zona["lon_max"]
            },
            "intentos": 0,
            "timestamp_origen": time.time()
        }
        
        if tipo_q == "q4":
            zona_b = random.choice(ZONAS)
            while zona_b["id"] == zona["id"]: 
                zona_b = random.choice(ZONAS)
            mensaje["params"].update({
                "lat_min_a": zona["lat_min"], "lat_max_a": zona["lat_max"], 
                "lon_min_a": zona["lon_min"], "lon_max_a": zona["lon_max"],
                "lat_min_b": zona_b["lat_min"], "lat_max_b": zona_b["lat_max"], 
                "lon_min_b": zona_b["lon_min"], "lon_max_b": zona_b["lon_max"]
            })
            for k in ["lat_min", "lat_max", "lon_min", "lon_max"]: 
                mensaje["params"].pop(k, None)

        if tipo_q == "q5": 
            mensaje["params"]["bins"] = 5

        producer.produce(
            TOPIC_PRINCIPAL, 
            json.dumps(mensaje).encode('utf-8'), 
            callback=delivery_report
        )
        
        producer.poll(0)
        
        if i % 50 == 0:
            print(f"[{i}/{TOTAL_CONSULTAS}] Mensajes encolados en Kafka", flush=True)

    producer.flush()
    print("las consultas fueron publicadas", flush=True)

if __name__ == "__main__":
    print("Esperando...", flush=True)
    time.sleep(10)
    
    generar_consultas("uniforme")