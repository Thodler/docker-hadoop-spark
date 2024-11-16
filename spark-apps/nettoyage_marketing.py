from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower, col, when

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

df_marketing = spark.sql("SELECT * FROM marketing_ext")
df_marketing = df_marketing.filter(df_marketing['sexe'] != 'sexe')

df_marketing = df_marketing.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "�", "e"))
df_marketing = df_marketing.withColumn("situationfamiliale", lower(col("situationfamiliale")))

df_marketing = df_marketing.withColumn(
    "taux_eligible",
    when((df_marketing["taux"] < taux_inf) | (df_marketing["taux"] > taux_supp), True).otherwise(False)
)

table_name = "marketing"

table_exists = spark._jsparkSession.catalog().tableExists("concessionnaire", table_name)

if not table_exists:
    print('Création et enregistrement de la table "marketing"')
    df_marketing.write.saveAsTable(table_name)
else:
    print('Enregistrement dans la table "marketing"')
    df_marketing.write.mode("overwrite").saveAsTable(table_name)