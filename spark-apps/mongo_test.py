# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pymongo import MongoClient
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# Créer une session Spark avec le support Hive
spark = SparkSession.builder \
    .appName("MongoDB vers Hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Connexion à MongoDB
client = MongoClient("mongodb://mongodb:27017/")
db = client['concessionnaire']
collection = db['immatriculations']

# Récupérer les données de MongoDB
data = list(collection.find())

# Définir le schéma correspondant à votre classe Immatriculations
schema = StructType([
    StructField("immatriculation", StringType(), True),
    StructField("marque", StringType(), True),
    StructField("nom", StringType(), True),
    StructField("puissance", IntegerType(), True),
    StructField("longueur", StringType(), True),
    StructField("nbPlaces", IntegerType(), True),
    StructField("nbPortes", IntegerType(), True),
    StructField("couleur", StringType(), True),
    StructField("occasion", BooleanType(), True),
    StructField("prix", IntegerType(), True)
])

# Créer le DataFrame Spark à partir des données
df = spark.createDataFrame(data, schema=schema)

# Écrire le DataFrame dans HDFS au format Parquet
df.write.mode("overwrite").parquet("/user/hive/warehouse/immatriculations")

# Créer une table externe Hive qui pointe vers les données en Parquet
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS immatriculations (
        immatriculation STRING,
        marque STRING,
        nom STRING,
        puissance INT,
        longueur STRING,
        nbPlaces INT,
        nbPortes INT,
        couleur STRING,
        occasion BOOLEAN,
        prix INT
    )
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/immatriculations'
""")

# Arrêter la session Spark
spark.stop()
