import os
import sys
import urllib.request

# ==========================================
# 0. CONFIGURATION SPÉCIFIQUE WINDOWS
# ==========================================
os.environ['PYSPARK_PYTHON'] = sys.executable

# Téléchargement automatique de winutils.exe et hadoop.dll si exécuté sur Windows
hadoop_dir = os.path.join(os.path.expanduser("~"), "hadoop")
hadoop_bin = os.path.join(hadoop_dir, "bin")
os.makedirs(hadoop_bin, exist_ok=True)

winutils_exe = os.path.join(hadoop_bin, "winutils.exe")
hadoop_dll = os.path.join(hadoop_bin, "hadoop.dll")

if not os.path.exists(winutils_exe):
    try:
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe",
            winutils_exe
        )
    except Exception as e:
        print(f"Note: winutils non téléchargé ({e}) - Ignoré sous Linux/Docker.")

if not os.path.exists(hadoop_dll):
    try:
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll",
            hadoop_dll
        )
    except Exception as e:
        print(f"Note: hadoop.dll non téléchargé ({e}) - Ignoré sous Linux/Docker.")

os.environ["HADOOP_HOME"] = hadoop_dir
os.environ["PATH"] += os.pathsep + hadoop_bin

# Adresse Redpanda (local par défaut, ou via Docker)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")

# ==========================================
# 1. INITIALISATION PYSPARK (STABLE 3.5.1)
# ==========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder \
    .appName("POC_Tickets_Export") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("ticket_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("demande", StringType(), True),
    StructField("type_demande", StringType(), True),
    StructField("priorite", StringType(), True)
])

# 2. Lecture du flux Redpanda
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Parsing JSON et Enrichissement
df_tickets = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("ticket_id").isNotNull())

df_enrichi = df_tickets.withColumn(
    "equipe_support",
    when(col("type_demande") == "Incident technique", "Équipe Infrastructure")
    .when(col("type_demande") == "Facturation", "Équipe Comptabilité")
    .otherwise("Équipe Support Général")
)

# 4. Écriture continue (Export au format JSON)
query = df_enrichi.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output_json") \
    .option("checkpointLocation", "checkpoints_json") \
    .start()

print(f"=== SPARK EN ECOUTE SUR {KAFKA_SERVER} - EXPORT JSON DANS output_json/ ===")
query.awaitTermination()