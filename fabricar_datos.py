import argparse
import json
import os
import random
import urllib.request
from datetime import datetime, timedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

os.makedirs('data', exist_ok=True)


def fetch_es_data(es_url="http://localhost:9200", index="metricas_sistema"):
    """Obtiene los documentos del índice de Elasticsearch."""
    query = {
        "size": 1000,
        "sort": [{"@timestamp": {"order": "asc"}}],
        "_source": [
            "@timestamp", "throughput_por_minuto", "latencia_p50", "latencia_p95",
            "hit_rate_pct", "retry_rate_pct", "total_consultas", "total_fallidos",
            "avg_cola_ms"
        ]
    }
    url  = f"{es_url}/{index}/_search"
    data = json.dumps(query).encode("utf-8")
    req  = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            resp = json.loads(r.read())
            hits = resp.get("hits", {}).get("hits", [])
            return [h["_source"] for h in hits]
    except Exception as e:
        print(f"[INFO] No se pudo conectar a ES: {e} → usando datos simulados.")
        return []


def parse_es_docs(docs):
    """Convierte los documentos ES a arrays numpy para graficar."""
    ts           = [datetime.fromisoformat(d["@timestamp"].replace("Z","")) for d in docs]
    throughput   = [d.get("throughput_por_minuto", 0) or 0 for d in docs]
    lat_p50      = [d.get("latencia_p50", 0) or 0 for d in docs]
    lat_p95      = [d.get("latencia_p95", 0) or 0 for d in docs]
    hit_rate     = [d.get("hit_rate_pct", 0) or 0 for d in docs]
    retry_rate   = [d.get("retry_rate_pct", 0) or 0 for d in docs]
    fallidos     = [d.get("total_fallidos", 0) or 0 for d in docs]
    cola_ms      = [d.get("avg_cola_ms", 0) or 0 for d in docs]
    return ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate, fallidos, cola_ms




def simular_escenario_normal(n=40):
    inicio = datetime(2026, 6, 21, 20, 0, 0)
    ts         = [inicio + timedelta(seconds=i*30) for i in range(n)]
    throughput = [random.uniform(80, 120) for _ in range(n)]
    lat_p50    = [random.uniform(4, 8) for _ in range(n)]
    lat_p95    = [random.uniform(20, 40) for _ in range(n)]
    hit_rate   = [random.uniform(80, 90) for _ in range(n)]
    retry_rate = [random.uniform(0, 2) for _ in range(n)]
    fallidos   = [random.randint(0, 3) for _ in range(n)]
    cola_ms    = [random.uniform(10, 50) for _ in range(n)]
    return ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate, fallidos, cola_ms


def simular_escenario_falla(n=60):
    inicio = datetime(2026, 6, 21, 20, 30, 0)
    ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate, fallidos, cola_ms = [], [], [], [], [], [], [], []
    for i in range(n):
        t = inicio + timedelta(seconds=i*30)
        ts.append(t)
        if i < 20:          
            throughput.append(random.uniform(80, 110))
            lat_p50.append(random.uniform(4, 8))
            lat_p95.append(random.uniform(20, 40))
            hit_rate.append(random.uniform(80, 90))
            retry_rate.append(random.uniform(0, 2))
            fallidos.append(random.randint(0, 2))
            cola_ms.append(random.uniform(10, 50))
        elif i < 40:        
            throughput.append(random.uniform(0, 20))
            lat_p50.append(0)
            lat_p95.append(0)
            hit_rate.append(random.uniform(70, 85))  
            retry_rate.append(random.uniform(60, 95))
            fallidos.append(random.randint(20, 50))
            cola_ms.append(random.uniform(5000, 15000))
        else:               
            throughput.append(random.uniform(60, 110))
            lat_p50.append(random.uniform(5, 15))
            lat_p95.append(random.uniform(30, 80))
            hit_rate.append(random.uniform(78, 88))
            retry_rate.append(random.uniform(5, 15))
            fallidos.append(random.randint(0, 5))
            cola_ms.append(random.uniform(50, 500))
    return ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate, fallidos, cola_ms


def simular_escenario_spike(n=60):
    inicio = datetime(2026, 6, 21, 21, 0, 0)
    ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate, fallidos, cola_ms = [], [], [], [], [], [], [], []
    for i in range(n):
        t = inicio + timedelta(seconds=i*30)
        ts.append(t)
        if 20 <= i <= 40:   
            throughput.append(random.uniform(300, 500))
            lat_p50.append(random.uniform(20, 60))
            lat_p95.append(random.uniform(100, 300))
            hit_rate.append(random.uniform(60, 75))
            retry_rate.append(random.uniform(10, 30))
            fallidos.append(random.randint(10, 30))
            cola_ms.append(random.uniform(1000, 8000))
        else:
            throughput.append(random.uniform(80, 120))
            lat_p50.append(random.uniform(4, 8))
            lat_p95.append(random.uniform(20, 40))
            hit_rate.append(random.uniform(80, 90))
            retry_rate.append(random.uniform(0, 2))
            fallidos.append(random.randint(0, 3))
            cola_ms.append(random.uniform(10, 50))
    return ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate, fallidos, cola_ms


STYLE = {
    'figure.facecolor':  '#1e1e2e',
    'axes.facecolor':    '#2a2a3e',
    'axes.edgecolor':    '#555577',
    'axes.labelcolor':   '#cdd6f4',
    'xtick.color':       '#cdd6f4',
    'ytick.color':       '#cdd6f4',
    'text.color':        '#cdd6f4',
    'grid.color':        '#45475a',
    'grid.linestyle':    '--',
    'grid.alpha':        0.5,
    'legend.facecolor':  '#313244',
    'legend.edgecolor':  '#555577',
}

def setup_ax(ax, title, ylabel, xlabel="Tiempo"):
    ax.set_title(title, fontsize=13, fontweight='bold', pad=10)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha='right')
    ax.grid(True)
    ax.legend(fontsize=9)


def graficar_escenario(ts, throughput, lat_p50, lat_p95, hit_rate, retry_rate,
                       fallidos, cola_ms, titulo, archivo):
    """Genera un dashboard de 6 subgráficos para un escenario."""
    with plt.rc_context(STYLE):
        fig, axes = plt.subplots(3, 2, figsize=(16, 12))
        fig.suptitle(titulo, fontsize=16, fontweight='bold', y=1.01)
        plt.tight_layout(pad=3.0)

        ax = axes[0, 0]
        ax.plot(ts, throughput, color='#89b4fa', linewidth=2, label='Consultas/min')
        ax.fill_between(ts, throughput, alpha=0.2, color='#89b4fa')
        setup_ax(ax, "Throughput (consultas exitosas/min)", "Consultas/min")

        ax = axes[0, 1]
        ax.plot(ts, lat_p50, color='#a6e3a1', linewidth=2, label='p50 (ms)')
        ax.plot(ts, lat_p95, color='#f38ba8', linewidth=2, linestyle='--', label='p95 (ms)')
        ax.fill_between(ts, lat_p50, lat_p95, alpha=0.15, color='#fab387')
        setup_ax(ax, "Latencia p50 / p95", "Latencia (ms)")

        ax = axes[1, 0]
        ax.plot(ts, hit_rate, color='#94e2d5', linewidth=2, label='Hit Rate (%)')
        ax.axhline(y=80, color='#f9e2af', linestyle=':', linewidth=1.5, label='Objetivo 80%')
        ax.set_ylim(0, 105)
        setup_ax(ax, "Cache Hit Rate (%)", "Hit Rate (%)")

        ax = axes[1, 1]
        ax.plot(ts, retry_rate, color='#fab387', linewidth=2, label='Retry Rate (%)')
        ax.fill_between(ts, retry_rate, alpha=0.2, color='#fab387')
        ax.set_ylim(0, max(max(retry_rate) * 1.2, 5))
        setup_ax(ax, "Retry Rate (%)", "Retry Rate (%)")

        ax = axes[2, 0]
        ax.bar(ts, fallidos, width=0.0003, color='#f38ba8', alpha=0.8, label='Fallidas por ventana')
        setup_ax(ax, "Consultas Fallidas / Ventana", "Cantidad")

        ax = axes[2, 1]
        ax.plot(ts, cola_ms, color='#cba6f7', linewidth=2, label='Avg cola (ms)')
        ax.fill_between(ts, cola_ms, alpha=0.2, color='#cba6f7')
        setup_ax(ax, "Tiempo Promedio en Cola Kafka (ms)", "ms")

        plt.savefig(f"data/{archivo}", dpi=150, bbox_inches='tight',
                    facecolor=STYLE['figure.facecolor'])
        plt.close()
        print(f"  ✓ {archivo}")



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--simular', action='store_true', help='Fuerza datos simulados')
    parser.add_argument('--es-url', default='http://localhost:9200')
    args = parser.parse_args()

    print("Generando gráficos del sistema...")

    docs = [] if args.simular else fetch_es_data(args.es_url)

    if docs:
        print(f"[ES] {len(docs)} documentos recuperados desde Elasticsearch.")
        datos = parse_es_docs(docs)
        graficar_escenario(*datos, "Dashboard Real — Elasticsearch", "informe_0_realtime.png")
    else:
        print("[INFO] Usando datos simulados para todos los escenarios.")

    escenarios = [
        (simular_escenario_normal, "Escenario Normal — Operación Base",        "informe_1_throughput.png"),
        (simular_escenario_falla,  "Escenario Falla — Caída y Recuperación",   "informe_2_latencia.png"),
        (simular_escenario_spike,  "Escenario Spike — Pico de Tráfico",        "informe_3_backlog_spike.png"),
    ]

    for fn, titulo, archivo in escenarios:
        datos = fn()
        graficar_escenario(*datos, titulo, archivo)

    with plt.rc_context(STYLE):
        fig, ax = plt.subplots(figsize=(14, 5))
        for fn, label, color in [
            (simular_escenario_normal, "Normal",  '#89b4fa'),
            (simular_escenario_falla,  "Falla",   '#f38ba8'),
            (simular_escenario_spike,  "Spike",   '#fab387'),
        ]:
            ts, throughput, *_ = fn()
            ax.plot(ts, throughput, color=color, linewidth=2, label=label)

        ax.set_title("Comparativa Throughput — Todos los Escenarios", fontsize=14, fontweight='bold')
        ax.set_xlabel("Tiempo"); ax.set_ylabel("Consultas exitosas/min")
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha='right')
        ax.grid(True); ax.legend(fontsize=10)
        plt.tight_layout()
        plt.savefig("data/informe_4_comparativa.png", dpi=150, bbox_inches='tight',
                    facecolor=STYLE['figure.facecolor'])
        plt.close()
        print("  ✓ informe_4_comparativa.png")

    print("\n¡Gráficos listos en la carpeta data/!")
    print("  Escenario real (si ES disponible): data/informe_0_realtime.png")
    print("  Normal:      data/informe_1_throughput.png")
    print("  Falla:       data/informe_2_latencia.png")
    print("  Spike:       data/informe_3_backlog_spike.png")
    print("  Comparativa: data/informe_4_comparativa.png")


if __name__ == "__main__":
    main()