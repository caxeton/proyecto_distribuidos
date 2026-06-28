import time
import json
import uuid
import requests
import redis
from confluent_kafka import Consumer, Producer

KAFKA_BROKER = "kafka:9092"
TOPIC_PRINCIPAL = "consultas_geo"
TOPIC_REINTENTOS = "consultas_reintentos"
TOPIC_DLQ = "consultas_dlq"
TOPIC_METRICS = "metrics-topic"          
URL_CEREBRO = "http://generador_respuestas:5000"
MAX_REINTENTOS = 3

cache = redis.Redis(host='sistema_cache', port=6379, db=0, decode_responses=True)

producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all',                        
    'retries': 3,
})

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'grupo_procesadores',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'session.timeout.ms': 30000,          
    'heartbeat.interval.ms': 10000,
})
consumer.subscribe([TOPIC_PRINCIPAL, TOPIC_REINTENTOS])

print("Consumidor esperando mensajes...", flush=True)



def generar_cache_key(tipo_q: str, params: dict) -> str | None:
    """Recrea la cache key exacta que usa Flask para leer Redis directamente."""
    conf = params.get("confidence_min", 0.0)
    if tipo_q == "q4":
        zona_a = f"{params['lat_min_a']}_{params['lat_max_a']}_{params['lon_min_a']}_{params['lon_max_a']}"
        zona_b = f"{params['lat_min_b']}_{params['lat_max_b']}_{params['lon_min_b']}_{params['lon_max_b']}"
        return f"compare:density:{zona_a}:{zona_b}:conf={conf}"
    zona = f"{params.get('lat_min')}_{params.get('lat_max')}_{params.get('lon_min')}_{params.get('lon_max')}"
    keys = {
        "q1": f"count:{zona}:conf={conf}",
        "q2": f"area:{zona}:conf={conf}",
        "q3": f"density:{zona}:conf={conf}",
        "q5": f"confidence_dist:{zona}:bins={params.get('bins', 5)}",
    }
    return keys.get(tipo_q)


def publicar_metrica(evento: dict) -> None:
    """Publica un evento de métrica en metrics-topic para que Spark lo consuma."""
    try:
        producer.produce(
            TOPIC_METRICS,
            json.dumps(evento).encode('utf-8'),
        )
        producer.poll(0)
    except Exception as e:
        print(f"[METRICS] Error publicando métrica: {e}", flush=True)


def derivar_a_falla(datos: dict) -> None:
    """Maneja reintentos y Dead Letter Queue."""
    datos['intentos'] = datos.get('intentos', 0) + 1   
    destino = TOPIC_REINTENTOS if datos['intentos'] <= MAX_REINTENTOS else TOPIC_DLQ
    etiqueta = f"REINTENTO {datos['intentos']}/{MAX_REINTENTOS}" if destino == TOPIC_REINTENTOS else "DLQ"
    print(f"[{etiqueta}] {datos['id_consulta']} → {destino}", flush=True)
    producer.produce(destino, json.dumps(datos).encode('utf-8'))
    producer.flush()



while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"[ERROR Kafka] {msg.error()}", flush=True)
        continue

    try:
        datos = json.loads(msg.value().decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"[ERROR JSON] mensaje inválido: {e}", flush=True)
        continue

    tipo_q      = datos.get("tipo", "")
    params      = datos.get("params", {})
    id_consulta = datos.get("id_consulta", str(uuid.uuid4()))
    timestamp_origen = datos.get("timestamp_origen", time.time())
    intentos    = datos.get("intentos", 0)

    inicio = time.time()
    tiempo_en_cola_ms = (inicio - timestamp_origen) * 1000

    cache_key = generar_cache_key(tipo_q, params)
    cache_hit = False

    if cache_key:
        try:
            valor = cache.get(cache_key)
            cache_hit = valor is not None
        except redis.RedisError as e:
            print(f"[ERROR Redis] {e}", flush=True)

    if cache_hit:
        fin = time.time()
        latencia_ms = (fin - inicio) * 1000
        print(f"[HIT]  {id_consulta} | lat={latencia_ms:.2f}ms | cola={tiempo_en_cola_ms:.2f}ms", flush=True)

        publicar_metrica({
            "event_id":        str(uuid.uuid4()),
            "timestamp":       time.time(),
            "id_consulta":     id_consulta,
            "tipo_consulta":   tipo_q,
            "latencia_ms":     latencia_ms,
            "cache_hit":       True,
            "reintentos":      intentos,
            "estado":          "exitoso",
            "tiempo_cola_ms":  tiempo_en_cola_ms,
        })
        continue

    try:
        url      = f"{URL_CEREBRO}/{tipo_q}"
        respuesta = requests.get(url, params=params, timeout=5.0)
        fin       = time.time()
        latencia_ms = (fin - inicio) * 1000

        if respuesta.status_code == 200:
            print(f"[MISS] {id_consulta} | lat={latencia_ms:.2f}ms | cola={tiempo_en_cola_ms:.2f}ms", flush=True)

            publicar_metrica({
                "event_id":       str(uuid.uuid4()),
                "timestamp":      time.time(),
                "id_consulta":    id_consulta,
                "tipo_consulta":  tipo_q,
                "latencia_ms":    latencia_ms,
                "cache_hit":      False,
                "reintentos":     intentos,
                "estado":         "exitoso",
                "tiempo_cola_ms": tiempo_en_cola_ms,
            })

        else:
            print(f"[HTTP {respuesta.status_code}] {id_consulta}", flush=True)
            publicar_metrica({
                "event_id":       str(uuid.uuid4()),
                "timestamp":      time.time(),
                "id_consulta":    id_consulta,
                "tipo_consulta":  tipo_q,
                "latencia_ms":    latencia_ms,
                "cache_hit":      False,
                "reintentos":     intentos,
                "estado":         "fallido",
                "tiempo_cola_ms": tiempo_en_cola_ms,
            })
            derivar_a_falla(datos)

    except requests.exceptions.Timeout:
        print(f"[TIMEOUT] {id_consulta}", flush=True)
        publicar_metrica({
            "event_id":       str(uuid.uuid4()),
            "timestamp":      time.time(),
            "id_consulta":    id_consulta,
            "tipo_consulta":  tipo_q,
            "latencia_ms":    5000.0,
            "cache_hit":      False,
            "reintentos":     intentos,
            "estado":         "timeout",
            "tiempo_cola_ms": tiempo_en_cola_ms,
        })
        derivar_a_falla(datos)
        time.sleep(1)

    except requests.exceptions.RequestException as e:
        print(f"[RED] {id_consulta} → {e}", flush=True)
        publicar_metrica({
            "event_id":       str(uuid.uuid4()),
            "timestamp":      time.time(),
            "id_consulta":    id_consulta,
            "tipo_consulta":  tipo_q,
            "latencia_ms":    -1.0,
            "cache_hit":      False,
            "reintentos":     intentos,
            "estado":         "fallido",
            "tiempo_cola_ms": tiempo_en_cola_ms,
        })
        derivar_a_falla(datos)
        time.sleep(1)