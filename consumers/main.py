import time
import json
import requests
import redis
from confluent_kafka import Consumer, Producer

KAFKA_BROKER = "kafka:9092"
TOPIC_PRINCIPAL = "consultas_geo"
URL_CEREBRO = "http://generador_respuestas:5000"

cache = redis.Redis(host='sistema_cache', port=6379, db=0)

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'grupo_procesadores',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([TOPIC_PRINCIPAL])

print("👷 Consumidor listo y esperando mensajes", flush=True)

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error de Kafka: {msg.error()}", flush=True)
        continue

    datos = json.loads(msg.value().decode('utf-8'))
    tipo_q = datos["tipo"]
    params = datos["params"]
    
    
    print(f"Procesando {tipo_q} para {datos['id_consulta']}...", flush=True)
    try:
        url = f"{URL_CEREBRO}/{tipo_q}"
        respuesta = requests.get(url, params=params, timeout=5.0)
        
        if respuesta.status_code == 200:
            print(f"Éxito: {datos['id_consulta']} calculada.", flush=True)
        else:
            print(f"Fallo para {datos['id_consulta']}", flush=True)
            
    except requests.exceptions.RequestException:
        print(f"Servidor caído. Perdimos: {datos['id_consulta']}", flush=True)