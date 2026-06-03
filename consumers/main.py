import time
import json
import requests
import redis
from confluent_kafka import Consumer, Producer

KAFKA_BROKER = "kafka:9092"
TOPIC_PRINCIPAL = "consultas_geo"
URL_CEREBRO = "http://generador_respuestas:5000"
TOPIC_REINTENTOS = "consultas_reintentos"
TOPIC_DLQ = "consultas_dlq"
MAX_REINTENTOS = 3

cache = redis.Redis(host='sistema_cache', port=6379, db=0)

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'grupo_procesadores',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([TOPIC_PRINCIPAL, TOPIC_REINTENTOS])

print("consumidor esperando mensajes", flush=True)

def generar_cache_key(tipo_q, params):
    """Recrea la llave exacta que usa Flask para poder leer Redis directamente"""
    conf = params.get("confidence_min", 0.0)
    if tipo_q == "q4":
        zona_a = f"{params['lat_min_a']}_{params['lat_max_a']}_{params['lon_min_a']}_{params['lon_max_a']}"
        zona_b = f"{params['lat_min_b']}_{params['lat_max_b']}_{params['lon_min_b']}_{params['lon_max_b']}"
        return f"compare:density:{zona_a}:{zona_b}:conf={conf}"
    else:
        zona = f"{params['lat_min']}_{params['lat_max']}_{params['lon_min']}_{params['lon_max']}"
        if tipo_q == "q1": return f"count:{zona}:conf={conf}"
        if tipo_q == "q2": return f"area:{zona}:conf={conf}"
        if tipo_q == "q3": return f"density:{zona}:conf={conf}"
        if tipo_q == "q5": return f"confidence_dist:{zona}:bins={params.get('bins', 5)}"
    return None

def derivar_a_falla(datos):
    """Maneja la lógica de reintentos y Dead Letter Queue (DLQ)"""
    datos['intentos'] += 1
    if datos['intentos'] <= MAX_REINTENTOS:
        print(f"reintento {datos['intentos']}/{MAX_REINTENTOS} para {datos['id_consulta']}. Enviando a cola de reintentos...", flush=True)
        producer.produce(TOPIC_REINTENTOS, json.dumps(datos).encode('utf-8'))
    else:
        print(f"maximo de reintentos alcanzado. {datos['id_consulta']} enviada a la DLQ.", flush=True)
        producer.produce(TOPIC_DLQ, json.dumps(datos).encode('utf-8'))
    producer.flush()



while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error de Kafka: {msg.error()}", flush=True)
        continue

    datos = json.loads(msg.value().decode('utf-8'))
    tipo_q = datos["tipo"]
    params = datos["params"]
    id_consulta = datos["id_consulta"]
    
    cache_key = generar_cache_key(tipo_q, params)
    if cache_key and cache.get(cache_key):
        print(f"Respuesta encontrada en caché para {id_consulta}", flush=True)
        continue 

    try:
        url = f"{URL_CEREBRO}/{tipo_q}"
        respuesta = requests.get(url, params=params, timeout=5.0)
        
        if respuesta.status_code == 200:
            print(f"exito: {datos['id_consulta']} calculada.", flush=True)
        else:
            print(f"fallo para {datos['id_consulta']}", flush=True)
            derivar_a_falla(datos)
            
    except requests.exceptions.RequestException:
        print(f"Servidor caído. Perdimos: {datos['id_consulta']}", flush=True)
        derivar_a_falla(datos)
        time.sleep(1) 

