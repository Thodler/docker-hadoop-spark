##########
# Nettoyage initial
##########

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, when, trim, split, regexp_replace, round, lower, col, encode, count, levenshtein, row_number, broadcast, coalesce, udf
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

df_catalogue = df_catalogue.filter(df_catalogue['marque'] != 'marque') # A GERER DANS L'IMPORT DE HIVE ???

# co2 dispose de marque et modele dans la meme colone
df_co2 = df_co2.withColumn("marque", split(df_co2["marque_modele"], " ", 2).getItem(0))
df_co2 = df_co2.withColumn("modele", split(df_co2["marque_modele"], " ", 2).getItem(1))
df_co2 = df_co2.drop('marque_modele')

# La colone **nom** du catalogue n'est pas nommée **modele** dans dans **co2**
df_catalogue = df_catalogue.withColumnRenamed("nom", "modele")

# La colone modele dans les 2 tableaux n’ont pas la meme casse.
df_co2 = df_co2.withColumn("marque", lower(trim(col("marque"))))
df_catalogue = df_catalogue.withColumn("marque", lower(trim(col("marque"))))

# La colone modele dans les 2 tableaux n’ont pas la meme casse.
df_co2 = df_co2.withColumn("modele", lower(trim(col("modele"))))
df_catalogue = df_catalogue.withColumn("modele", lower(trim(col("modele"))))

# Le signe € est mentionné dans la colone bonus_malus de co2.
df_co2 = df_co2.withColumn("bonus_malus", split(trim(df_co2["bonus_malus"]), "€").getItem(0))
# Le signe € est mentionné dans la colone cout_energie de co2.
df_co2 = df_co2.withColumn("cout_energie", split(trim(df_co2["cout_energie"]), "€").getItem(0))

# Le chiffre 1 peut apparaitre après le signe € dans la colone bonus_malus de co2.
df_co2 = df_co2.withColumn("bonus_malus", regexp_replace(trim(df_co2["bonus_malus"]), "[^0-9-]", "").cast("float"))

#Le symbole "�" apparaît dans la colonne longueur.
df_catalogue = df_catalogue.withColumn("longueur", regexp_replace(col("longueur"), "�", "e"))

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
