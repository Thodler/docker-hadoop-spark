from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when, trim, split, regexp_replace, round

spark = SparkSession.builder\
    .appName("AggregateCatalogueCo2") \
    .config('hive.metastore.warehouse.dir', '/user/hive/warehouse') \
    .config('hive.metastore.uris', 'thrift://hive-metastore:9083') \
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

df_catalogue = spark.sql("SELECT * FROM catalogue_ext")
df_co2 = spark.sql("SELECT * FROM crit_air_ext")

df_catalogue = df_catalogue.filter(df_catalogue['marque'] != 'marque')

##########
# Nettoyage initial
##########

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, trim, regexp_replace, col, split

spark = SparkSession.builder\
    .appName("AggregateCatalogueCo2")\
    .config('hive.metastore.warehouse.dir', '/user/hive/warehouse')\
    .config('hive.metastore.uris', 'thrift://hive-metastore:9083')\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

# Chargement des données
df_catalogue = spark.sql("SELECT * FROM catalogue_ext")
df_co2 = spark.sql("SELECT * FROM crit_air_ext")

# Nettoyage initial
df_catalogue = df_catalogue.filter(df_catalogue['marque'] != 'marque')

# Traitement des colonnes avec nettoyage rigoureux
df_co2 = df_co2.withColumn("bonus_malus", split(trim(df_co2["bonus_malus"]), "€").getItem(0))
df_co2 = df_co2.withColumn("cout_energie", split(trim(df_co2["cout_energie"]), "€").getItem(0))
df_co2 = df_co2.withColumn("bonus_malus", regexp_replace(trim(df_co2["bonus_malus"]), "[^0-9-]", "").cast("float"))
df_co2 = df_co2.withColumn("cout_energie", regexp_replace(trim(df_co2["cout_energie"]), "[^0-9]", "").cast("float"))
df_co2 = df_co2.withColumn("rejets_co2", col("rejets_co2").cast("float"))


# traitement de bonus_malus
df_co2 = df_co2.withColumn("marque", split(df_co2["marque_modele"], " ", 2).getItem(0))
df_co2 = df_co2.withColumn("model", split(df_co2["marque_modele"], " ", 2).getItem(1))
df_co2 = df_co2.drop('marque_modele')

df_co2_agg = df_co2.groupby("marque").agg(round(avg("bonus_malus")).alias("moyenne_bonus_malus_marque"))

df_co2 = df_co2.join(df_co2_agg, on="marque")

# df_catalogue.show()
df_co2.show()
