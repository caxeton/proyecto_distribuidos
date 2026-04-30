import pandas as pd
from flask import Flask, request, jsonify
import redis
import json
import numpy as np
import math

app = Flask(__name__)

cache = redis.Redis(host='sistema_cache', port=6379, db=0)

print("Cargando dataset en memoria...")
try:
    df = pd.read_csv('/app/data/data.csv')
    df = df.dropna(subset=['latitude', 'longitude', 'confidence'])
    print(f"Dataset cargado exitosamente. Total de registros: {len(df)}")
except Exception as e:
    print(f"Error al cargar el dataset: {e}")
    df = pd.DataFrame()

def generar_zona_id(lat_min, lat_max, lon_min, lon_max):
    return f"{lat_min}_{lat_max}_{lon_min}_{lon_max}"

def filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min=0.0):
    return df[
        (df['latitude'] >= lat_min) & (df['latitude'] <= lat_max) &
        (df['longitude'] >= lon_min) & (df['longitude'] <= lon_max) &
        (df['confidence'] >= conf_min)
    ]

def calcular_area_km2(lat_min, lat_max, lon_min, lon_max):
    lat_promedio = math.radians((lat_min + lat_max) / 2.0)
    alto_km = abs(lat_max - lat_min) * 111.32
    ancho_km = abs(lon_max - lon_min) * 111.32 * math.cos(lat_promedio)
    return alto_km * ancho_km

# VARIABLE MÁGICA: 1.5 Megabytes de peso simulado
PESO_ARTIFICIAL = "x" * 1500000 

# --- Q1 ---
@app.route('/q1', methods=['GET'])
def q1_count():
    lat_min, lat_max = float(request.args.get('lat_min')), float(request.args.get('lat_max'))
    lon_min, lon_max = float(request.args.get('lon_min')), float(request.args.get('lon_max'))
    conf_min = float(request.args.get('confidence_min', 0.0))
    
    zona_id = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"count:{zona_id}:conf={conf_min}"
    
    cached = cache.get(cache_key)
    if cached: 
        res = json.loads(cached)
        res.pop("payload", None) # TRUCO: Quitamos el peso antes de enviarlo por red
        res["status"] = "HIT"
        return jsonify(res)

    filtro = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min)
    res = {"consulta": "Q1", "total_edificios": len(filtro), "status": "MISS"}
    
    # TRUCO: Guardamos en Redis CON el peso gigante para saturarlo
    cache_data = res.copy()
    cache_data["payload"] = PESO_ARTIFICIAL
    cache.setex(cache_key, 10, json.dumps(cache_data))
    
    return jsonify(res)

# --- Q2 ---
@app.route('/q2', methods=['GET'])
def q2_area():
    lat_min, lat_max = float(request.args.get('lat_min')), float(request.args.get('lat_max'))
    lon_min, lon_max = float(request.args.get('lon_min')), float(request.args.get('lon_max'))
    conf_min = float(request.args.get('confidence_min', 0.0))
    
    zona_id = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"area:{zona_id}:conf={conf_min}"
    
    cached = cache.get(cache_key)
    if cached: 
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min)
    avg_area = float(filtro['area_in_meters'].mean()) if not filtro.empty else 0
    total_area = float(filtro['area_in_meters'].sum()) if not filtro.empty else 0
    
    res = {"consulta": "Q2", "avg_area": avg_area, "total_area": total_area, "n": len(filtro), "status": "MISS"}
    
    cache_data = res.copy()
    cache_data["payload"] = PESO_ARTIFICIAL
    cache.setex(cache_key, 10, json.dumps(cache_data))
    return jsonify(res)

# --- Q3 ---
@app.route('/q3', methods=['GET'])
def q3_density():
    lat_min, lat_max = float(request.args.get('lat_min')), float(request.args.get('lat_max'))
    lon_min, lon_max = float(request.args.get('lon_min')), float(request.args.get('lon_max'))
    conf_min = float(request.args.get('confidence_min', 0.0))
    
    zona_id = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"density:{zona_id}:conf={conf_min}"
    
    cached = cache.get(cache_key)
    if cached: 
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min)
    count = len(filtro)
    area_km2 = calcular_area_km2(lat_min, lat_max, lon_min, lon_max)
    densidad = count / area_km2 if area_km2 > 0 else 0
    
    res = {"consulta": "Q3", "density_per_km2": densidad, "area_km2": area_km2, "count": count, "status": "MISS"}
    
    cache_data = res.copy()
    cache_data["payload"] = PESO_ARTIFICIAL
    cache.setex(cache_key, 10, json.dumps(cache_data))
    return jsonify(res)

# --- Q4 ---
@app.route('/q4', methods=['GET'])
def q4_compare():
    lat_min_a, lat_max_a = float(request.args.get('lat_min_a')), float(request.args.get('lat_max_a'))
    lon_min_a, lon_max_a = float(request.args.get('lon_min_a')), float(request.args.get('lon_max_a'))
    lat_min_b, lat_max_b = float(request.args.get('lat_min_b')), float(request.args.get('lat_max_b'))
    lon_min_b, lon_max_b = float(request.args.get('lon_min_b')), float(request.args.get('lon_max_b'))
    conf_min = float(request.args.get('confidence_min', 0.0))

    zona_a = generar_zona_id(lat_min_a, lat_max_a, lon_min_a, lon_max_a)
    zona_b = generar_zona_id(lat_min_b, lat_max_b, lon_min_b, lon_max_b)
    cache_key = f"compare:density:{zona_a}:{zona_b}:conf={conf_min}"
    
    cached = cache.get(cache_key)
    if cached: 
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro_a = filtrar_datos(lat_min_a, lat_max_a, lon_min_a, lon_max_a, conf_min)
    area_a = calcular_area_km2(lat_min_a, lat_max_a, lon_min_a, lon_max_a)
    da = len(filtro_a) / area_a if area_a > 0 else 0

    filtro_b = filtrar_datos(lat_min_b, lat_max_b, lon_min_b, lon_max_b, conf_min)
    area_b = calcular_area_km2(lat_min_b, lat_max_b, lon_min_b, lon_max_b)
    db = len(filtro_b) / area_b if area_b > 0 else 0

    winner = "zona_a" if da > db else "zona_b"
    if da == db: winner = "empate"
    
    res = {"consulta": "Q4", "zona_a_density": da, "zona_b_density": db, "winner": winner, "status": "MISS"}
    
    cache_data = res.copy()
    cache_data["payload"] = PESO_ARTIFICIAL
    cache.setex(cache_key, 10, json.dumps(cache_data))
    return jsonify(res)

# --- Q5 ---
@app.route('/q5', methods=['GET'])
def q5_confidence_dist():
    lat_min, lat_max = float(request.args.get('lat_min')), float(request.args.get('lat_max'))
    lon_min, lon_max = float(request.args.get('lon_min')), float(request.args.get('lon_max'))
    bins = int(request.args.get('bins', 5))
    
    zona_id = generar_zona_id(lat_min, lat_max, lon_min, lon_max)
    cache_key = f"confidence_dist:{zona_id}:bins={bins}"
    
    cached = cache.get(cache_key)
    if cached: 
        res = json.loads(cached)
        res.pop("payload", None)
        res["status"] = "HIT"
        return jsonify(res)

    filtro = filtrar_datos(lat_min, lat_max, lon_min, lon_max, conf_min=0.0)
    if filtro.empty:
        res = {"consulta": "Q5", "histograma": [], "status": "MISS"}
    else:
        counts, edges = np.histogram(filtro['confidence'], bins=bins, range=(0, 1))
        histograma = [{"bucket": i, "min": float(edges[i]), "max": float(edges[i+1]), "count": int(counts[i])} for i in range(bins)]
        res = {"consulta": "Q5", "histograma": histograma, "status": "MISS"}

    cache_data = res.copy()
    cache_data["payload"] = PESO_ARTIFICIAL
    cache.setex(cache_key, 10, json.dumps(cache_data))
    return jsonify(res)
@app.route('/stats', methods=['GET'])


@app.route('/flush', methods=['GET'])
def flush_cache():
    try:
        cache.flushall()
        return jsonify({"Status" : "Cache eliminado correctamente"})
    except Exception as e:
        return jsonify({"error": str(e)})

def get_stats():
    try:
        info = cache.info()
        evicciones= info.get('evicted_keys', 0)
        return jsonify({"evictions": evicciones})
    except Exception as e:
        print(f"Error consultando: {e}", flush=True)
        return jsonify({"evictions": 0})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)