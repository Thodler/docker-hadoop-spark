from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# Définir un schéma personnalisé pour gérer les conversions de types
customSchema = StructType([
    StructField("_id", StructType([
        StructField("oid", StringType(), True)
    ]), True),
    StructField("couleur", StringType(), True),
    StructField("immatriculation", StringType(), True),
    StructField("longueur", StringType(), True),
    StructField("marque", StringType(), True),
    StructField("nbPlaces", StringType(), True),  # Initialement en String
    StructField("nbPortes", StringType(), True),  # Initialement en String
    StructField("nom", StringType(), True),
    StructField("occasion", StringType(), True),
    StructField("prix", StringType(), True),  # Initialement en String
    StructField("puissance", StringType(), True)  # Initialement en String
])

# Initialisation de la session Spark avec les configurations MongoDB et Hive
spark = SparkSession.builder \
    .appName("MongoToHive") \
    .config("spark.mongodb.input.uri", "mongodb://mongodb:27017/concessionnaire.immatriculations") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/concessionnaire.immatriculations") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.driver.memoryOverhead", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

# Chargement des données depuis MongoDB avec le schéma personnalisé
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://mongodb:27017/concessionnaire.immatriculations") \
    .schema(customSchema) \
    .load()

# Conversion des champs de type String en Boolean ou Integer selon le besoin
df_converted = df \
    .withColumn("nbPlaces", col("nbPlaces").cast(IntegerType())) \
    .withColumn("nbPortes", col("nbPortes").cast(IntegerType())) \
    .withColumn("prix", col("prix").cast(IntegerType())) \
    .withColumn("puissance", col("puissance").cast(IntegerType())) \
    .withColumn("occasion", when(col("occasion") == 'true', True).otherwise(False))  # Exemple de conversion

# Optionnel : Afficher le schéma pour vérifier les types
df_converted.printSchema()

# Répartition des données pour optimiser l'utilisation de la mémoire
df_repartitioned = df_converted.repartition(10)  # Ajustez le nombre de partitions selon vos besoins

# Écrire les données dans une table Hive en format Parquet
df_repartitioned.write.mode("overwrite").format("parquet").saveAsTable("concessionnaire.immatriculations_ext")

# Fermeture de la session Spark
spark.stop()