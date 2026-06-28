#!/usr/bin/env python3
import json
import time
import urllib.request
import urllib.error

ES_HOST  = "elasticsearch"
ES_PORT  = 9200
INDEX    = "metricas_sistema"
URL_BASE = f"http://{ES_HOST}:{ES_PORT}"


MAPPING = {
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
        "refresh_interval":   "5s"
    },
    "mappings": {
        "properties": {
            "@timestamp":            {"type": "date"},
            "window_start":          {"type": "date"},
            "window_end":            {"type": "date"},
            "total_consultas":       {"type": "long"},
            "total_exitosas":        {"type": "long"},
            "total_fallidos":        {"type": "long"},
            "throughput_por_minuto": {"type": "float"},
            "latencia_p50":          {"type": "float"},
            "latencia_p95":          {"type": "float"},
            "hit_rate_pct":          {"type": "float"},
            "retry_rate_pct":        {"type": "float"},
            "avg_cola_ms":           {"type": "float"},
            "count_q1":              {"type": "long"},
            "count_q2":              {"type": "long"},
            "count_q3":              {"type": "long"},
            "count_q4":              {"type": "long"},
            "count_q5":              {"type": "long"},
        }
    }
}


def wait_for_es(max_retries: int = 30, delay: int = 5) -> bool:
    for i in range(max_retries):
        try:
            req = urllib.request.Request(f"{URL_BASE}/_cluster/health")
            with urllib.request.urlopen(req, timeout=3) as r:
                health = json.loads(r.read())
                status = health.get("status", "red")
                if status in ("green", "yellow"):
                    print(f"[ES Init] Elasticsearch listo (status={status})", flush=True)
                    return True
        except Exception:
            pass
        print(f"[ES Init] Esperando ES... intento {i+1}/{max_retries}", flush=True)
        time.sleep(delay)
    return False


def create_index() -> None:
    url = f"{URL_BASE}/{INDEX}"

    try:
        req = urllib.request.Request(url, method="HEAD")
        urllib.request.urlopen(req, timeout=5)
        print(f"[ES Init] Índice '{INDEX}' ya existe. Nada que hacer.", flush=True)
        return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"[ES Init] Error verificando índice: {e}", flush=True)
            return

    data    = json.dumps(MAPPING).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method="PUT")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
            if resp.get("acknowledged"):
                print(f"[ES Init] Índice '{INDEX}' creado exitosamente.", flush=True)
            else:
                print(f"[ES Init] Respuesta inesperada: {resp}", flush=True)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"[ES Init] Error creando índice: {e.code} → {body}", flush=True)


if __name__ == "__main__":
    print("[ES Init] Iniciando configuración de Elasticsearch...", flush=True)
    if wait_for_es():
        create_index()
    else:
        print("[ES Init] Elasticsearch no respondió a tiempo. Abortando.", flush=True)
        raise SystemExit(1)