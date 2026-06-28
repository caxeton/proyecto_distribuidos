set -euo pipefail

ESCENARIO="${1:-normal}"
WORKERS="${2:-1}"

log() { echo -e "\n\033[1;36m[$(date '+%H:%M:%S')] $*\033[0m"; }
ok()  { echo -e "\033[1;32m✓ $*\033[0m"; }
err() { echo -e "\033[1;31m✗ $*\033[0m" >&2; }

log "═══════════════════════════════════════════════"
log " ESCENARIO: $ESCENARIO | WORKERS: $WORKERS"
log "═══════════════════════════════════════════════"

log "[1/6] Limpiando entorno anterior..."
docker compose down -v --remove-orphans 2>/dev/null || true
ok "Entorno limpio"

log "[2/6] Levantando infraestructura (Kafka, Redis, Elasticsearch, Kibana)..."
docker compose up -d zookeeper kafka sistema_cache elasticsearch kibana
ok "Infraestructura iniciada"

log "[3/6] Esperando servicios core (hasta 90s)..."

INTENTOS=0
until docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do
    echo "  Kafka no listo aún..."; sleep 5
    INTENTOS=$((INTENTOS+1))
    if [ $INTENTOS -ge 18 ]; then echo "Kafka tardó demasiado"; exit 1; fi
done
echo "  Kafka OK"

INTENTOS=0
until curl -s http://localhost:9200/_cluster/health 2>/dev/null | grep -q "yellow\|green"; do
    echo "  Elasticsearch no listo aún..."; sleep 5
    INTENTOS=$((INTENTOS+1))
    if [ $INTENTOS -ge 18 ]; then echo "ES tardó demasiado"; exit 1; fi
done
echo "  Elasticsearch OK"

ok "Kafka y Elasticsearch listos"

log "[4/6] Creando topics de Kafka..."
for TOPIC in consultas_geo consultas_reintentos consultas_dlq metrics-topic; do
    docker exec kafka kafka-topics \
        --create --if-not-exists \
        --topic "$TOPIC" \
        --partitions 3 \
        --replication-factor 1 \
        --bootstrap-server localhost:9092 2>/dev/null && ok "  Topic: $TOPIC"
done

log "[5/6] Levantando Flask, consumidores y Spark..."
docker compose up -d generador_respuestas es_init


log "  Esperando Flask (hasta 3 minutos)..."
INTENTOS=0
until curl -sf http://localhost:5005/health &>/dev/null; do
    echo "  Flask cargando dataset... ($((INTENTOS * 3))s)"
    sleep 3
    INTENTOS=$((INTENTOS+1))
    if [ $INTENTOS -ge 60 ]; then
        echo "Flask tardó demasiado, revisando logs..."
        docker logs cerebro_flask --tail 5
        exit 1
    fi
done
ok "Flask listo"

sleep 5

docker compose up -d spark_streaming
ok "Spark iniciado"

log "  Levantando $WORKERS consumidor(es)..."
docker compose up -d --scale trabajador_1="$WORKERS" trabajador_1
ok "$WORKERS consumidor(es) activos"

sleep 5

log "[6/6] Ejecutando escenario: $ESCENARIO"

case "$ESCENARIO" in

  normal)
    docker compose run --rm \
      -e TOTAL_CONSULTAS=5000 \
      -e DISTRIBUCION=uniforme \
      -e MODO=normal \
      generador_trafico
    ;;

  zipf)
    docker compose run --rm \
      -e TOTAL_CONSULTAS=5000 \
      -e DISTRIBUCION=zipf \
      -e MODO=normal \
      generador_trafico
    ;;

  multi)
    log "  Escalando a 3 consumidores..."
    docker compose up -d --scale trabajador_1=3 trabajador_1
    sleep 3
    docker compose run --rm \
      -e TOTAL_CONSULTAS=5000 \
      -e DISTRIBUCION=uniforme \
      -e MODO=normal \
      generador_trafico
    ;;

  spike)
    docker compose run --rm \
      -e TOTAL_CONSULTAS=5000 \
      -e DISTRIBUCION=uniforme \
      -e MODO=spike \
      generador_trafico
    ;;

  falla)
    log "  Iniciando tráfico en background y simulando falla..."
    docker compose run --rm \
      -e TOTAL_CONSULTAS=5000 \
      -e DISTRIBUCION=uniforme \
      -e MODO=normal \
      -e DELAY_MS=5 \
      generador_trafico &
    TRAFICO_PID=$!

    sleep 10
    log "  >>> SIMULANDO CAÍDA de cerebro_flask <<<"; docker stop cerebro_flask
    sleep 15
    log "  >>> RECUPERANDO cerebro_flask <<<";        docker start cerebro_flask

    wait $TRAFICO_PID
    ;;

  dlq)
    log "  Tráfico con fallas repetidas para forzar DLQ..."
    docker compose run --rm \
      -e TOTAL_CONSULTAS=2000 \
      -e DISTRIBUCION=uniforme \
      -e MODO=normal \
      -e DELAY_MS=10 \
      generador_trafico &
    TRAFICO_PID=$!

    for i in 1 2 3; do
        sleep 8
        log "  Ciclo de falla $i/3"; docker stop cerebro_flask; sleep 5; docker start cerebro_flask
    done
    wait $TRAFICO_PID
    ;;

  *)
    err "Escenario desconocido: $ESCENARIO"
    echo "Uso: ./ejecutar.sh [normal|zipf|multi|spike|falla|dlq] [num_workers]"
    exit 1
    ;;
esac

ok "═══════════════════════════════════════════════"
ok " Escenario '$ESCENARIO' completado"
ok " Kibana disponible en: http://localhost:5601"
ok " Elasticsearch:        http://localhost:9200"
ok " Flask API:            http://localhost:5005"
ok "═══════════════════════════════════════════════"
echo ""
echo "Para ver logs de Spark:     docker logs -f spark_streaming"
echo "Para ver logs de consumers: docker compose logs -f trabajador_1"
echo "Para ver métricas en ES:    curl http://localhost:9200/metricas_sistema/_search?pretty"