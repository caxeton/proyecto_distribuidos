import time
import random
import requests
import numpy as np

URL_BASE = "http://generador_respuestas:5000"
TOTAL_CONSULTAS = 200

ZONAS = [
    {"id": "Z1", "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    {"id": "Z2", "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    {"id": "Z3", "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    {"id": "Z4", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    {"id": "Z5", "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760}
]

TIPOS_CONSULTAS = ["q1", "q2", "q3", "q4", "q5"]

def obtener_evictions():
    try:
        r = requests.get(f"{URL_BASE}/stats", timeout=5.0)
        evicciones = r.json().get("evictions", 0)
        return evicciones
    except Exception as e:
        print(f"\n[ERROR CRÍTICO] No se pudo leer evictions del servidor: {e}\n", flush=True)
        return 0

def ejecutar_experimento(tipo_distribucion="uniforme"):
    print(f"\n=======================================================", flush=True)
    print(f"INICIANDO: {tipo_distribucion.upper()}", flush=True)
    print(f"=======================================================", flush=True)
    
    tiempos_totales = []
    latencias_hit = []   
    latencias_miss = []  
    resultados = {"HIT": 0, "MISS": 0}
    
    if tipo_distribucion == "zipf":
        a = 1.5
        probabilidades = [1.0 / (i**a) for i in range(1, len(ZONAS) + 1)]
        probabilidades /= np.sum(probabilidades)
    
    evictions_start = obtener_evictions()
    tiempo_inicio_total = time.time()

    for i in range(TOTAL_CONSULTAS):
        if tipo_distribucion == "uniforme":
            zona = random.choice(ZONAS)
        else:
            zona = np.random.choice(ZONAS, p=probabilidades)
        
        tipo_q = random.choice(TIPOS_CONSULTAS)
        url = f"{URL_BASE}/{tipo_q}"
        params = {"confidence_min": 0.7}
        
        if tipo_q == "q4":
            zona_b = random.choice(ZONAS)
            while zona_b["id"] == zona["id"]: zona_b = random.choice(ZONAS)
            params.update({
                "lat_min_a": zona["lat_min"], "lat_max_a": zona["lat_max"],
                "lon_min_a": zona["lon_min"], "lon_max_a": zona["lon_max"],
                "lat_min_b": zona_b["lat_min"], "lat_max_b": zona_b["lat_max"],
                "lon_min_b": zona_b["lon_min"], "lon_max_b": zona_b["lon_max"]
            })
            info_zona = f"{zona['id']} vs {zona_b['id']}"
        else:
            params.update({
                "lat_min": zona["lat_min"], "lat_max": zona["lat_max"],
                "lon_min": zona["lon_min"], "lon_max": zona["lon_max"]
            })
            if tipo_q == "q5": params["bins"] = 5
            info_zona = f"Zona {zona['id']}"

        inicio_req = time.time()
        try:
            respuesta = requests.get(url, params=params, timeout=5.0, headers={'Connection': 'close'})
            fin_req = time.time()
            
            latencia_ms = (fin_req - inicio_req) * 1000
            tiempos_totales.append(latencia_ms)
            
            datos = respuesta.json()
            status = datos.get("status", "MISS")
            resultados[status] += 1
            
            if status == "HIT": latencias_hit.append(latencia_ms)
            else: latencias_miss.append(latencia_ms)
            
            if i % 100 == 0:
                print(f"[{i}/{TOTAL_CONSULTAS}] Procesando...", flush=True)
            
        except Exception as e:
            pass 
            
    tiempo_fin_total = time.time()
    tiempo_total_segundos = tiempo_fin_total - tiempo_inicio_total
    minutos_transcurridos = tiempo_total_segundos / 60.0

    evictions_end = obtener_evictions()
    evictions_del_periodo = max(0, evictions_end - evictions_start)

    if len(tiempos_totales) > 0:
        hit_rate = (resultados["HIT"] / len(tiempos_totales)) * 100
        throughput = len(tiempos_totales) / tiempo_total_segundos
        p50 = np.percentile(tiempos_totales, 50)
        p95 = np.percentile(tiempos_totales, 95)
        eviction_rate = evictions_del_periodo / minutos_transcurridos if minutos_transcurridos > 0 else 0
        
        t_cache = np.mean(latencias_hit) if latencias_hit else 0
        t_db = np.mean(latencias_miss) if latencias_miss else 0
        total = resultados["HIT"] + resultados["MISS"]
        cache_efficiency = (resultados["HIT"] * t_cache - resultados["MISS"] * t_db) / total if total > 0 else 0
        
        tabla_resumen = (
            f"--- {tipo_distribucion.upper()} ---\n"
            f"Hit rate:          {hit_rate:.2f}%\n"
            f"Throughput:        {throughput:.2f} req/segundo\n"
            f"Latencia p50:      {p50:.2f} ms\n"
            f"Latencia p95:      {p95:.2f} ms\n"
            f"Eviction rate:     {eviction_rate:.2f} evictions/minuto\n"
            f"Cache efficiency:  {cache_efficiency:.2f}\n"
        )
        return tabla_resumen
    return "No hubo datos."

if __name__ == "__main__":
    print("Esperando 10 segundos para iniciar", flush=True)
    time.sleep(10)
    
    resumen_uniforme = ejecutar_experimento("uniforme")
    
    print("\n🧹 Eliminando caché para nueva tanda de consultas", flush=True)
    try:
        requests.get(f"{URL_BASE}/flush", timeout=5.0)
    except:
        print("Error elimnando caché.")
    time.sleep(5)
    
    resumen_zipf = ejecutar_experimento("zipf")
    
    print("\n\n" + "*"*50, flush=True)
    print(" Estaditicas", flush=True)
    print("*"*50, flush=True)
    print(resumen_uniforme, flush=True)
    print("-" * 30, flush=True)
    print(resumen_zipf, flush=True)
    print("*"*50, flush=True)   