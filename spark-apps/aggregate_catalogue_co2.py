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
df_catalogue = df_catalogue.withColumnRenamed("nom", "modele")

df_catalogue = df_catalogue.withColumn("marque", lower(trim(col("marque")))) \
                            .withColumn("modele", lower(trim(col("modele"))))

df_co2 = df_co2.withColumn("marque_modele", lower(trim(col("marque_modele"))))

# Traitement des colonnes avec nettoyage rigoureux
df_co2 = df_co2.withColumn("bonus_malus", split(trim(df_co2["bonus_malus"]), "€").getItem(0))
df_co2 = df_co2.withColumn("cout_energie", split(trim(df_co2["cout_energie"]), "€").getItem(0))
df_co2 = df_co2.withColumn("bonus_malus", regexp_replace(trim(df_co2["bonus_malus"]), "[^0-9-]", "").cast("float"))
df_co2 = df_co2.withColumn("cout_energie", regexp_replace(trim(df_co2["cout_energie"]), "[^0-9]", "").cast("float"))
df_co2 = df_co2.withColumn("rejets_co2", col("rejets_co2").cast("float"))


# traitement de bonus_malus
df_co2 = df_co2.withColumn("marque", split(df_co2["marque_modele"], " ", 2).getItem(0))
df_co2 = df_co2.withColumn("modele", split(df_co2["marque_modele"], " ", 2).getItem(1))
df_co2 = df_co2.drop('marque_modele')

df_co2_agg_bonus_malus = df_co2.groupby("marque").agg(round(avg("bonus_malus")).alias("moyenne_bonus_malus_marque"))
df_co2_agg_rejets_co2 = df_co2.groupby("marque").agg(round(avg("rejets_co2")).alias("moyenne_rejets_co2_marque"))
df_co2_agg_cout_energie = df_co2.groupby("marque").agg(round(avg("cout_energie")).alias("moyenne_cout_energie_marque"))

df_co2 = df_co2.join(df_co2_agg_bonus_malus, on="marque")
df_co2 = df_co2.join(df_co2_agg_rejets_co2, on="marque")
df_co2 = df_co2.join(df_co2_agg_cout_energie, on="marque")

df_catalogue_with_moyennes = df_catalogue.join(df_co2, on=["marque"], how="left")
# df_co2.show()
df_catalogue.show()

df_catalogue_with_moyennes.show(n=1000, truncate=False)
