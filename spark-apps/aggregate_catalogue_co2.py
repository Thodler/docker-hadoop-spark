from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when, trim, split, regexp_replace

spark = SparkSession.builder\
    .appName("AggregateCatalogueCo2") \
    .config('hive.metastore.warehouse.dir', '/user/hive/warehouse') \
    .config('hive.metastore.uris', 'thrift://hive-metastore:9083') \
    .enableHiveSupport()\
    .getOrCreate()

spark.catalog.clearCache()
spark.sql("USE concessionnaire")

df_catalogue = spark.sql("SELECT * FROM catalogue_ext")
df_co2 = spark.sql("SELECT * FROM crit_air_ext")

df_catalogue = df_catalogue.filter(df_catalogue['marque'] != 'marque')

##########
# Nettoyage initial
##########

# traitement de bonus_malus
df_co2 = df_co2.withColumn("bonus_malus",
                           regexp_replace(df_co2["bonus_malus"], "[^0-9.-]", "")
                          .cast("float"))
df_co2 = df_co2.withColumn("bonus_malus", when(df_co2["bonus_malus"].rlike("^-?\\d+(\\.\\d+)?€?$"), df_co2["bonus_malus"]).cast("float"))

# Traitement de La colone cout_energie
df_co2 = df_co2.withColumn("cout_energie", regexp_replace("cout_energie", "\u00A0", ""))
df_co2 = df_co2.withColumn("cout_energie", split(df_co2["cout_energie"], "€").getItem(0).cast("float"))

# Traitement de rejets_co2
df_co2 = df_co2.withColumn("rejets_co2", df_co2["rejets_co2"].cast("float"))

df_catalogue.show()
df_co2.show()
