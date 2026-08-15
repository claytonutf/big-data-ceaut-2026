#--- Como executar (2):
'''
conda activate ceaut

cd kafka-pyspark-dashboard

set PYSPARK_PYTHON=%CONDA_PREFIX%\python.exe

set PYSPARK_DRIVER_PYTHON=%CONDA_PREFIX%\python.exe

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 streaming-pyspark.py
    
'''


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    round
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "input"

CHECKPOINT_PATH = "./checkpoints/input_stream"
OUTPUT_PATH = "./data/output"


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("IndustrialMachineStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# SCHEMA DO JSON
# ============================================================

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("part_id", StringType(), True),
    StructField("operator", StringType(), True),

    StructField("cycle_time", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("power_consumption", DoubleType(), True),

    StructField("rpm", IntegerType(), True),

    StructField("tool_number", StringType(), True),
    StructField("tool_wear", DoubleType(), True),

    StructField("status", StringType(), True),
    StructField("error_code", StringType(), True),

    StructField("production_count", IntegerType(), True)
])


# ============================================================
# LEITURA DO KAFKA
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)


# ============================================================
# KAFKA VALUE → STRING
# ============================================================

json_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json")
)


# ============================================================
# JSON → STRUCT
# ============================================================

parsed_stream = (
    json_stream
    .select(
        from_json(
            col("json"),
            schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# TRANSFORMAÇÕES
# ============================================================

processed_stream = (
    parsed_stream

    # String → timestamp
    .withColumn(
        "timestamp",
        to_timestamp(
            col("timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # --------------------------------------------------------
    # Indicador de temperatura
    # --------------------------------------------------------
    .withColumn(
        "temperature_alert",
        when(col("temperature") >= 45, lit(True))
        .otherwise(lit(False))
    )

    # --------------------------------------------------------
    # Indicador de vibração
    # --------------------------------------------------------
    .withColumn(
        "vibration_alert",
        when(col("vibration") >= 0.50, lit(True))
        .otherwise(lit(False))
    )

    # --------------------------------------------------------
    # Indicador de desgaste da ferramenta
    # --------------------------------------------------------
    .withColumn(
        "tool_wear_alert",
        when(col("tool_wear") >= 80, lit(True))
        .otherwise(lit(False))
    )

    # --------------------------------------------------------
    # Indicador geral de anomalia
    # --------------------------------------------------------
    .withColumn(
        "machine_alert",
        when(
            (col("temperature") >= 45)
            | (col("vibration") >= 0.50)
            | (col("tool_wear") >= 80)
            | (col("status") == "ERROR")
            | col("error_code").isNotNull(),
            lit(True)
        )
        .otherwise(lit(False))
    )

    # --------------------------------------------------------
    # Consumo de energia por peça
    # --------------------------------------------------------
    .withColumn(
        "energy_per_part",
        when(
            col("production_count") > 0,
            round(
                col("power_consumption")
                / col("production_count"),
                3
            )
        ).otherwise(None)
    )
)


# ============================================================
# SELEÇÃO FINAL
# ============================================================

final_stream = processed_stream.select(
    "timestamp",
    "machine_id",
    "machine_type",
    "part_id",
    "operator",

    "cycle_time",
    "temperature",
    "vibration",
    "power_consumption",
    "rpm",

    "tool_number",
    "tool_wear",

    "status",
    "error_code",
    "production_count",

    "temperature_alert",
    "vibration_alert",
    "tool_wear_alert",
    "machine_alert",

    "energy_per_part"
)


# ============================================================
# OUTPUT
# ============================================================

query = (
    final_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "data/parquet"
    )
    .option(
        "checkpointLocation",
        "checkpoints/parquet"
    )
    .trigger(
        processingTime="2 seconds"
    )
    .start()
)


print("==========================================")
print(" PySpark Structured Streaming")
print("==========================================")
print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Tópico: {KAFKA_TOPIC}")
print(f"Output: {OUTPUT_PATH}")
print("Streaming iniciado...")
print("==========================================")


query.awaitTermination()