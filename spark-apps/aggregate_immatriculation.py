from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.functions import trim, lower, col, count, regexp_replace

spark = SparkSession.builder\
    .appName("AggregateImmatriculationClient")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

# Chargement des données
df_immat = spark.sql("SELECT * FROM immatriculations_ext")

# Nettoyage initial
df_immat = df_immat.withColumn("marque", lower(trim(col("marque"))))
df_immat = df_immat.withColumn("modele", lower(trim(col("modele"))))

df_immat = df_immat.withColumn("longueur", regexp_replace(col("longueur"), "�", "e"))
df_immat = df_immat.withColumn("occasion", df_immat["occasion"].cast(BooleanType()))
