from pyspark.sql import SparkSession

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("MongoDB to Hive") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/concessionnaire.immatriculations") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .enableHiveSupport() \
    .getOrCreate()

# Lire les données de MongoDB
df = spark.read.format("mongo").load()

# Spécifier le répertoire HDFS pour stocker les données en format Parquet
hdfs_path = "/user/hive/warehouse/concessionnaire.db/immatriculations_ext"

# Sauvegarder les données en format Parquet dans HDFS
df.write.format("parquet").mode("append").option("path", "hdfs://namenode:9000/user/hive/warehouse/concessionnaire.db/immatriculations_ext").saveAsTable("immatriculations_ext")

# Afficher les premières lignes pour vérifier
df.show()

# Arrêter la session Spark
spark.stop()