from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, avg, round
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, IndexToString
from pyspark.ml.classification import RandomForestClassifier

spark = SparkSession.builder\
    .appName("Préparation de données")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

df_catalogue = spark.sql("SELECT * FROM catalogue")

indexer = StringIndexer(inputCol="marque", outputCol="marque_index")
df_indexed = indexer.fit(df_catalogue).transform(df_catalogue)

encoder = OneHotEncoder(inputCol="marque_index", outputCol="marque_encoded")
df_encoded = encoder.fit(df_indexed).transform(df_indexed)

df_catalogue = df_catalogue.drop("marque")

df_catalogue = df_catalogue.join(df_encoded.select("marque_encoded"), how="inner").withColumnRenamed("marque_encoded", "marque")

count_modele = df_catalogue.select("modele").distinct().count()

indexer = StringIndexer(inputCol="categorie", outputCol="categorie_indexed")
indexer_model = indexer.fit(df_catalogue)
df_catalogue = indexer_model.transform(df_catalogue)

df_catalogue = df_catalogue.drop("categorie")
df_catalogue = df_catalogue.drop("modele")

df_catalogue = df_catalogue.withColumnRenamed("categorie_index", "categorie")

df_catalogue = df_catalogue.withColumn(
    "longueur",
    when(col("longueur") == "courte", 0)
    .when(col("longueur") == "moyenne", 1)
    .when(col("longueur") == "longue", 2)
    .when(col("longueur") == "tres longue", 3)
    .otherwise(None)
    .cast("int")
)
