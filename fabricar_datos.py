import csv
from datetime import datetime, timedelta
import random
import os

os.makedirs('data', exist_ok=True)

def generar_csv_simulado(archivo, total_mensajes, segundos_totales, max_espera_ms):
    """Genera un archivo CSV simulando el comportamiento físico de tu sistema distribuido."""
    with open(f'data/{archivo}', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'evento', 'latencia_ms', 'espera_cola_ms'])

        inicio = datetime(2026, 6, 3, 20, 0, 0)

        for i in range(total_mensajes):
            tiempo_actual = inicio + timedelta(seconds=(i / total_mensajes) * segundos_totales)
            
            # Simulamos el 85% de Cache HIT rate que viste en tus logs
            evento = "HIT" if random.random() > 0.15 else "MISS"
            latencia = random.uniform(3.0, 6.0) if evento == "HIT" else random.uniform(15.0, 80.0)
            
            espera_cola = (i / total_mensajes) * max_espera_ms

            writer.writerow([
                tiempo_actual.strftime("%Y-%m-%d %H:%M:%S.%f"), 
                evento, 
                round(latencia, 2), 
                round(espera_cola, 2)
            ])
            
def generar_csv_falla():
    """Genera la métrica del escenario donde apagaste el servidor."""
    with open('data/metricas_falla.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'evento', 'latencia_ms', 'espera_cola_ms'])
        inicio = datetime(2026, 6, 3, 20, 30, 0)
        
        for i in range(500):
            tiempo = inicio + timedelta(seconds=(i/500)*40)
            if i < 150 or i > 400:
                writer.writerow([tiempo.strftime("%Y-%m-%d %H:%M:%S.%f"), "HIT", random.uniform(3, 6), random.uniform(10, 50)])
            elif i > 350:
                writer.writerow([tiempo.strftime("%Y-%m-%d %H:%M:%S.%f"), "DLQ", 0.0, 0.0])
            else:
                writer.writerow([tiempo.strftime("%Y-%m-%d %H:%M:%S.%f"), "RETRY", 0.0, 0.0])

print("Fabricando métricas basadas en los resultados de tu consola...")

generar_csv_simulado('metricas_sincrono.csv', 5000, 145.0, 0.0)     
generar_csv_simulado('metricas_kafka_1c.csv', 5000, 85.2, 85215.0) 
generar_csv_simulado('metricas_kafka_3c.csv', 5000, 28.0, 15000.0)  
generar_csv_falla()                                                   

print("¡Archivos CSV listos en la carpeta data/!")