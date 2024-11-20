from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, avg, round, floor
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, IndexToString, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.sql.types import DoubleType
from itertools import product

spark = SparkSession.builder\
    .appName("Création du model - client categories")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

df_marketing = spark.sql("SELECT * FROM marketing")
df_marketing.printSchema()


df_marketing = indexer_sexe.transform(df_marketing)
df_marketing = encoder_sexe.transform(df_marketing)

df_marketing = indexer_situationfamiliale.transform(df_marketing)
df_marketing = encoder_situationfamiliale.transform(df_marketing)

df_marketing = df_marketing.withColumn(
    "deuxiemevoiture",
    when(col("deuxiemevoiture") == False, 0)
    .when(col("deuxiemevoiture") == True, 1)
    .otherwise(col("deuxiemevoiture").cast("int"))
)

df_marketing = df_marketing.withColumn(
    "taux_eligible",
    when(col("taux_eligible") == False, 0)
    .when(col("taux_eligible") == True, 1)
    .otherwise(col("taux_eligible").cast("int"))
)
df_marketing = df_marketing.select(feature_cols)

# Assembler les caractéristiques
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
prediction_data = assembler.transform(df_marketing).select("features")

# Effectuer les prédictions
marketingPrediction = model.transform(prediction_data)

# Mapper les prédictions avec les labels
label_to_category = IndexToString(
    inputCol="prediction",
    outputCol="predicted_category",
    labels=indexer_model.labels
)
result = label_to_category.transform(marketingPrediction)

# Afficher les résultats
result.show()

spark.stop()