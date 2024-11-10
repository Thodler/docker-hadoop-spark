from pyspark.sql import SparkSession

# Initialiser la session Spark avec le connecteur Cassandra
spark = SparkSession.builder \
    .appName("Cassandra to Hive") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
    .enableHiveSupport() \
    .getOrCreate()

# Lire les données de Cassandra
# Remplace "keyspace_name" par le nom de ton keyspace Cassandra, et "table_name" par la table source
df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="crit_air", keyspace="co2") \
    .load()

df.write.format("parquet").mode("overwrite").option("path", "hdfs://namenode:9000/user/hive/warehouse/concessionnaire.db/crit_air_ext").saveAsTable("crit_air_ext")

# Afficher les premières lignes pour vérifier
df.show()

# Si besoin, écrire les données au format Parquet dans HDFS
# df.write.parquet("hdfs://path/to/output")

# Arrêter la session Spark
spark.stop()