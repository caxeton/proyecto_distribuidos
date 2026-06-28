import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, BooleanType, IntegerType, DoubleType, TimestampType
)

KAFKA_BROKER    = os.getenv("KAFKA_BROKER",   "kafka:9092")
METRICS_TOPIC   = os.getenv("METRICS_TOPIC",  "metrics-topic")
ES_HOST         = os.getenv("ES_HOST",         "elasticsearch")
ES_PORT         = os.getenv("ES_PORT",         "9200")
ES_INDEX        = os.getenv("ES_INDEX",        "metricas_sistema")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
SLIDE_DURATION  = os.getenv("SLIDE_DURATION",  "30 seconds")
CHECKPOINT_DIR  = os.getenv("CHECKPOINT_DIR",  "/tmp/spark_checkpoint")

ES_NODES        = f"{ES_HOST}"
ES_RESOURCE     = ES_INDEX

SCHEMA = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("timestamp",      DoubleType(),  True),   
    StructField("id_consulta",    StringType(),  True),
    StructField("tipo_consulta",  StringType(),  True),
    StructField("latencia_ms",    FloatType(),   True),
    StructField("cache_hit",      BooleanType(), True),
    StructField("reintentos",     IntegerType(), True),
    StructField("estado",         StringType(),  True),
    StructField("tiempo_cola_ms", FloatType(),   True),
])


def create_spark_session() -> SparkSession:
    """Crea la SparkSession con todos los paquetes necesarios."""
    return (
        SparkSession.builder
        .appName("MetricasStreamingT3")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0")
        .config("es.nodes",           ES_NODES)
        .config("es.port",            ES_PORT)
        .config("es.nodes.wan.only",  "true")
        .config("es.index.auto.create","true")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def build_stream(spark: SparkSession):
    """Lee el stream crudo desde Kafka y lo parsea como JSON."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", METRICS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(
            F.from_json(F.col("json_str"), SCHEMA).alias("data"),
            F.col("kafka_ts")
        )
        .select("data.*", "kafka_ts")
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp").cast("long")))
        .withWatermark("event_ts", "2 minutes")   # tolerancia a eventos tardíos
    )
    return parsed


def aggregate_stream(parsed):
    """Aplica ventanas deslizantes y calcula las métricas agregadas."""

    agg = (
        parsed
        .groupBy(
            F.window("event_ts", WINDOW_DURATION, SLIDE_DURATION).alias("ventana")
        )
        .agg(
            F.count("*").alias("total_consultas"),

            F.sum(
                F.when(F.col("estado") == "exitoso", 1).otherwise(0)
            ).alias("total_exitosas"),

            F.sum(
                F.when(F.col("estado").isin("fallido", "timeout"), 1).otherwise(0)
            ).alias("total_fallidos"),

            F.percentile_approx(
                F.when(F.col("estado") == "exitoso", F.col("latencia_ms")),
                0.50
            ).alias("latencia_p50"),

            F.percentile_approx(
                F.when(F.col("estado") == "exitoso", F.col("latencia_ms")),
                0.95
            ).alias("latencia_p95"),

            F.avg(F.col("cache_hit").cast("integer")).alias("hit_rate_raw"),

            F.avg(
                F.when(F.col("reintentos") > 0, 1).otherwise(0)
            ).alias("retry_rate_raw"),

            F.avg("tiempo_cola_ms").alias("avg_cola_ms"),

            F.count(F.when(F.col("tipo_consulta") == "q1", 1)).alias("count_q1"),
            F.count(F.when(F.col("tipo_consulta") == "q2", 1)).alias("count_q2"),
            F.count(F.when(F.col("tipo_consulta") == "q3", 1)).alias("count_q3"),
            F.count(F.when(F.col("tipo_consulta") == "q4", 1)).alias("count_q4"),
            F.count(F.when(F.col("tipo_consulta") == "q5", 1)).alias("count_q5"),
        )
        .select(
            F.col("ventana.start").alias("window_start"),
            F.col("ventana.end").alias("window_end"),
            F.col("total_consultas"),
            F.col("total_exitosas"),
            F.col("total_fallidos"),
            (F.col("total_exitosas") /
             (F.unix_timestamp("ventana.end") - F.unix_timestamp("ventana.start")) * 60
            ).alias("throughput_por_minuto"),
            F.col("latencia_p50"),
            F.col("latencia_p95"),
            (F.col("hit_rate_raw")   * 100).alias("hit_rate_pct"),
            (F.col("retry_rate_raw") * 100).alias("retry_rate_pct"),
            F.col("avg_cola_ms"),
            F.col("count_q1"), F.col("count_q2"), F.col("count_q3"),
            F.col("count_q4"), F.col("count_q5"),
            F.date_format("ventana.start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("@timestamp"),
        )
    )
    return agg


def write_to_elasticsearch(batch_df, batch_id: int):
    """Escribe cada micro-batch en Elasticsearch."""
    if batch_df.isEmpty():
        return
    try:
        (
            batch_df.write
            .format("org.elasticsearch.spark.sql")
            .option("es.resource",        ES_RESOURCE)
            .option("es.nodes",           ES_NODES)
            .option("es.port",            ES_PORT)
            .option("es.nodes.wan.only",  "true")
            .option("es.mapping.id",      "@timestamp")   
            .mode("append")
            .save()
        )
        print(f"[Spark] batch {batch_id} → {batch_df.count()} docs escritos en ES", flush=True)
    except Exception as e:
        print(f"[Spark][ERROR] batch {batch_id}: {e}", flush=True)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Spark] Leyendo desde {METRICS_TOPIC} @ {KAFKA_BROKER}", flush=True)
    print(f"[Spark] Escribiendo en ES {ES_HOST}:{ES_PORT}/{ES_INDEX}", flush=True)

    parsed = build_stream(spark)
    agg    = aggregate_stream(parsed)

    query = (
        agg.writeStream
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .foreachBatch(write_to_elasticsearch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
    )

    print("[Spark] Stream iniciado. Esperando datos...", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()