

echo "[1/5] Limpiando el entorno"
docker compose down -v > /dev/null 2>&1

echo "[2/5] Levantando arquitectura (Kafka, Redis, Flask y 3 Workers)"
docker compose up -d --scale trabajador_1=3 > /dev/null 2>&1

echo "[3/5] Esperando 15 segundos para que Kafka inicialice por completo"
sleep 15

echo "[4/5] Configurando 3 particiones en Kafka para escalamiento horizontal"
docker exec kafka kafka-topics --create --topic consultas_geo --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 > /dev/null 2>&1

echo "[5/5] Inyectando 5.000 consultas de tráfico"
docker compose up -d generador_trafico


(
    sleep 10
    echo -e "\n\n>>> SIMULANDO CAÍDA DEL SERVIDOR (Apagando cerebro_flask en 3, 2, 1...) <<<\n\n"
    docker stop cerebro_flask > /dev/null
    
    sleep 15
    echo -e "\n\n>>> RECUPERANDO SERVIDOR (Encendiendo cerebro_flask...) <<<\n\n"
    docker start cerebro_flask > /dev/null
) &



docker compose logs -f trabajador_1