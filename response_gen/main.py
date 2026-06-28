import pandas as pd
from flask import Flask, request, jsonify
import redis
import json
import numpy as np
import math

app = Flask(__name__)

cache = redis.Redis(host='sistema_cache', port=6379, db=0, decode_responses=True)

print("Cargando dataset en memoria...", flush=True)
try:
    df = pd.read_csv('/app/data/data.csv')
    df = df.dropna(subset=['latitude', 'longitude', 'confidence'])
    df['latitude']      = pd.to_numeric(df['latitude'],      errors='coerce')
    df['longitude']     = pd.to_numeric(df['longitude'],     errors='coerce')
    df['confidence']    = pd.to_numeric(df['confidence'],    errors='coerce')
    df['area_in_meters']= pd.to_numeric(df['area_in_meters'],errors='coerce')
    df = df.dropna()
    print(f"Dataset cargado. Total registros: {len(df)}", flush=True)
except Exception as e:
    print(f"[ERROR] al cargar dataset: {e}", flush=True)
    df = pd.DataFrame(columns=['latitude','longitude','confidence','area_in_meters'])

PESO_ARTIFICIAL = "x" * 1_500_000



def generar_zona_id(lat_min, lat_max, lon_min, lon_max) -> str:
    return f"{lat_min}_{lat_max}_{lon_min}_{lon_max}"


def filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min=0.0) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    return df[
        (df['latitude']   >= lat_min) & (df['latitude']   <= lat_max) &
        (df['longitude']  >= lon_min) & (df['longitude']  <= lon_max) &
        (df['confidence'] >= conf_min)
    ]


def calcular_area_km2(lat_min, lat_max, lon_min, lon_max) -> float:
    lat_promedio = math.radians((lat_min + lat_max) / 2.0)
    alto_km  = abs(lat_max - lat_min) * 111.32
    ancho_km = abs(lon_max - lon_min) * 111.32 * math.cos(lat_promedio)
    return alto_km * ancho_km


def get_float(key, default=0.0):
    val = request.args.get(key, default)
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def get_int(key, default=5):
    val = request.args.get(key, default)
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


@app.route('/health', methods=['GET'])          
def health():
    return jsonify({"status": "ok"}), 200


@app.route('/q1', methods=['GET'])
def q1_count():
    lat_min  = get_float('lat_min')
    lat_max  = get_float('lat_max')
    lon_min  = get_float('lon_min')
    lon_max  = get_float('lon_max')
    conf_min = get_float('confidence_min', 0.0)

    zona_id   = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"count:{zona_id}:conf={conf_min}"

    cached = cache.get(cache_key)
    if cached:
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min)
    res = {"consulta": "Q1", "total_edificios": len(filtro), "status": "MISS"}
    cache_data = {**res, "payload": PESO_ARTIFICIAL}
    cache.setex(cache_key, 3600, json.dumps(cache_data))
    return jsonify(res)


@app.route('/q2', methods=['GET'])
def q2_area():
    lat_min  = get_float('lat_min')
    lat_max  = get_float('lat_max')
    lon_min  = get_float('lon_min')
    lon_max  = get_float('lon_max')
    conf_min = get_float('confidence_min', 0.0)

    zona_id   = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"area:{zona_id}:conf={conf_min}"

    cached = cache.get(cache_key)
    if cached:
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro    = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min)
    avg_area  = float(filtro['area_in_meters'].mean()) if not filtro.empty else 0.0
    total_area= float(filtro['area_in_meters'].sum())  if not filtro.empty else 0.0
    res = {"consulta": "Q2", "avg_area": avg_area, "total_area": total_area, "n": len(filtro), "status": "MISS"}
    cache_data = {**res, "payload": PESO_ARTIFICIAL}
    cache.setex(cache_key, 3600, json.dumps(cache_data))
    return jsonify(res)


@app.route('/q3', methods=['GET'])
def q3_density():
    lat_min  = get_float('lat_min')
    lat_max  = get_float('lat_max')
    lon_min  = get_float('lon_min')
    lon_max  = get_float('lon_max')
    conf_min = get_float('confidence_min', 0.0)

    zona_id   = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"density:{zona_id}:conf={conf_min}"

    cached = cache.get(cache_key)
    if cached:
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro   = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min)
    area_km2 = calcular_area_km2(lat_min, lat_max, lon_min, lon_max)
    densidad = len(filtro) / area_km2 if area_km2 > 0 else 0.0
    res = {"consulta": "Q3", "density_per_km2": densidad, "area_km2": area_km2, "count": len(filtro), "status": "MISS"}
    cache_data = {**res, "payload": PESO_ARTIFICIAL}
    cache.setex(cache_key, 3600, json.dumps(cache_data))
    return jsonify(res)


@app.route('/q4', methods=['GET'])
def q4_compare():
    lat_min_a = get_float('lat_min_a'); lat_max_a = get_float('lat_max_a')
    lon_min_a = get_float('lon_min_a'); lon_max_a = get_float('lon_max_a')
    lat_min_b = get_float('lat_min_b'); lat_max_b = get_float('lat_max_b')
    lon_min_b = get_float('lon_min_b'); lon_max_b = get_float('lon_max_b')
    conf_min  = get_float('confidence_min', 0.0)

    zona_a    = generar_zona_id(lat_min_a, lat_max_a, lon_min_a, lon_max_a)
    zona_b    = generar_zona_id(lat_min_b, lat_max_b, lon_min_b, lon_max_b)
    cache_key = f"compare:density:{zona_a}:{zona_b}:conf={conf_min}"

    cached = cache.get(cache_key)
    if cached:
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro_a = filtrar_datos(lat_min_a, lat_max_a, lon_min_a, lon_max_a, conf_min)
    area_a   = calcular_area_km2(lat_min_a, lat_max_a, lon_min_a, lon_max_a)
    da       = len(filtro_a) / area_a if area_a > 0 else 0.0

    filtro_b = filtrar_datos(lat_min_b, lat_max_b, lon_min_b, lon_max_b, conf_min)
    area_b   = calcular_area_km2(lat_min_b, lat_max_b, lon_min_b, lon_max_b)
    db       = len(filtro_b) / area_b if area_b > 0 else 0.0

    winner = "zona_a" if da > db else ("zona_b" if db > da else "empate")
    res = {"consulta": "Q4", "zona_a_density": da, "zona_b_density": db, "winner": winner, "status": "MISS"}
    cache_data = {**res, "payload": PESO_ARTIFICIAL}
    cache.setex(cache_key, 3600, json.dumps(cache_data))
    return jsonify(res)


@app.route('/q5', methods=['GET'])
def q5_confidence_dist():
    lat_min = get_float('lat_min')
    lat_max = get_float('lat_max')
    lon_min = get_float('lon_min')
    lon_max = get_float('lon_max')
    bins    = get_int('bins', 5)

    zona_id   = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"confidence_dist:{zona_id}:bins={bins}"

    cached = cache.get(cache_key)
    if cached:
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min=0.0)
    if filtro.empty:
        histograma = []
    else:
        counts, edges = np.histogram(filtro['confidence'], bins=bins, range=(0, 1))
        histograma = [
            {"bucket": i, "min": float(edges[i]), "max": float(edges[i+1]), "count": int(counts[i])}
            for i in range(bins)
        ]
    res = {"consulta": "Q5", "histograma": histograma, "status": "MISS"}
    cache_data = {**res, "payload": PESO_ARTIFICIAL}
    cache.setex(cache_key, 3600, json.dumps(cache_data))
    return jsonify(res)


@app.route('/stats', methods=['GET'])
def get_stats():
    try:
        info = cache.info()
        return jsonify({
            "evictions":  info.get('evicted_keys', 0),
            "hits":       info.get('keyspace_hits', 0),
            "misses":     info.get('keyspace_misses', 0),
            "used_memory":info.get('used_memory_human', 'N/A'),
        })
    except Exception as e:
        print(f"[ERROR] Redis stats: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route('/flush', methods=['GET'])
def flush_cache():
    try:
        cache.flushall()
        return jsonify({"status": "OK"})
    except Exception as e:
        return jsonify({"status": "ERROR", "detail": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)