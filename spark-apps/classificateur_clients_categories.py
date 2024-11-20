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

clients_immatriculations = spark.sql("SELECT * FROM clients_immatriculations")

clients_immatriculations = clients_immatriculations.drop('immatriculation')
clients_immatriculations = clients_immatriculations.drop('couleur')
clients_immatriculations = clients_immatriculations.drop('marque')
clients_immatriculations = clients_immatriculations.drop('puissance')
clients_immatriculations = clients_immatriculations.drop('nbportes')
clients_immatriculations = clients_immatriculations.drop('occasion')
clients_immatriculations = clients_immatriculations.drop('prix')
clients_immatriculations = clients_immatriculations.drop('nbplaces')
clients_immatriculations = clients_immatriculations.drop('modele')
clients_immatriculations = clients_immatriculations.drop('longueur')

# Transformation des colones
indexer_sexe = StringIndexer(inputCol="sexe", outputCol="sexe_index").fit(clients_immatriculations)
clients_immatriculations = indexer_sexe.transform(clients_immatriculations)

encoder_sexe = OneHotEncoder(inputCol="sexe_index", outputCol="sexe_encoded").fit(clients_immatriculations)
clients_immatriculations = encoder_sexe.transform(clients_immatriculations)

indexer_situationfamiliale = StringIndexer(inputCol="situationfamiliale", outputCol="situationfamiliale_index").fit(clients_immatriculations)
clients_immatriculations = indexer_situationfamiliale.transform(clients_immatriculations)

encoder_situationfamiliale = OneHotEncoder(inputCol="situationfamiliale_index", outputCol="situationfamiliale_encoded").fit(clients_immatriculations)
clients_immatriculations = encoder_situationfamiliale.transform(clients_immatriculations)


# Changer les boolean en INT
clients_immatriculations = clients_immatriculations.withColumn(
    "deuxiemevoiture",
    when(col("deuxiemevoiture") == False, 0)
    .when(col("deuxiemevoiture") == True, 1)
    .otherwise(col("deuxiemevoiture").cast("int"))
)

clients_immatriculations = clients_immatriculations.withColumn(
    "taux_eligible",
    when(col("taux_eligible") == False, 0)
    .when(col("taux_eligible") == True, 1)
    .otherwise(col("taux_eligible").cast("int"))
)

# Apprentissage des labels
indexer_model = StringIndexer(inputCol="categorie", outputCol="label").fit(clients_immatriculations)
clients_immatriculations = indexer_model.transform(clients_immatriculations)

feature_cols = [col for col in clients_immatriculations.columns if col not in ["categorie", "label", 'sexe', 'situationfamiliale']]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
data = assembler.transform(clients_immatriculations).select("features", "label")

(trainingData, testData) = data.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestClassifier(numTrees=10, maxDepth=15, maxBins=64, labelCol="label",
                            featuresCol="features")

# Entraîner le modèle
model = rf.fit(trainingData)

# Spécifier le chemin HDFS
hdfs_path = "hdfs://namenode:9000/user/hdfs/model/categorie"

# Sauvegarder le modèle dans HDFS
model.save(hdfs_path)

spark.stop()