from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.functions import trim, lower, col, count, regexp_replace, max, min, when,avg, round, rand

spark = SparkSession.builder\
    .appName("CleanImmatriculation")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")
spark.catalog.clearCache()
spark.sql("USE concessionnaire")

df_immat = spark.sql("SELECT * FROM immatriculations_ext")
df_client = spark.sql("SELECT * FROM clients")

#Renommage de la colonne "nom" en "modele"
df_immat = df_immat.withColumnRenamed("nom", "modele")

#Normalisation des marques et modèles
df_immat = df_immat.withColumn("marque", lower(trim(col("marque"))))
df_immat = df_immat.withColumn("modele", lower(trim(col("modele"))))

#Suppression de la 1ère ligne de la table clients
df_client = df_client.filter(df_client['immatriculation'] != 'immatriculation')

#Correction du symbole "�" dans la colonne "longueur"
df_immat = df_immat.withColumn("longueur", regexp_replace(col("longueur"), "�", "e"))
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "�", "e"))

#Mettre à null les valeurs inférieures à 18 ans.
df_client = df_client.withColumn("age", when(col("age") == -1, None).otherwise(col("age")))
df_client = df_client.withColumn("age", when(col("age") == 0, None).otherwise(col("age")))

#Calculer la médiane et remplacer les nulls par la médiane.
mediane = df_client.approxQuantile("age", [0.5], 0.01)[0]
df_client = df_client.withColumn("age", when(col("age").isNull(), mediane).otherwise(col("age")))

#Fusionner seul/seule en Celibataire
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "Seule", "Celibataire"))
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "Seul", "Celibataire"))
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "Divorcee", "Divorce"))

# Remplacer "N/D" par null dans la colonne situationfamiliale
df_client = df_client.withColumn(
    "situationfamiliale",
    when(col("situationfamiliale") == None, "N/D").otherwise(col("situationfamiliale"))
)
df_client = df_client.withColumn(
    "situationfamiliale",
    when(trim(col("situationfamiliale")) == "", "N/D").otherwise(col("situationfamiliale"))
)
# Remplacer "?" par "N/D" dans la colonne situationfamiliale
df_client = df_client.withColumn(
    "situationfamiliale",
    regexp_replace(col("situationfamiliale"), r"\?", "N/D")
)
#Supprimer les N/D car les proportions le permettent
df_client= df_client.filter(col("situationfamiliale") != "N/D")

# Remplacer les nulls de taux
mediane_taux = df_client.approxQuantile("taux", [0.5], 0.01)[0]
df_client = df_client.withColumn("taux", when(col("taux").isNull(), mediane).otherwise(col("taux")))

#Correction des valeurs hors domaine (Création d'une colonne "taux_eligible")
df_client = df_client.withColumn(
    "taux_eligible",
    when((col("taux") >= 544) & (col("taux") <= 74185), True).otherwise(False)
)

df_client = df_client.withColumn(
    "nbenfantacharge",
    when(
        (col("situationfamiliale") == "Celibataire") & (col("nbenfantacharge").isNull() | (col("nbenfantacharge") == -1)),
        0
    ).otherwise(col("nbenfantacharge"))
)

df_client = df_client.withColumn(
    "nbenfantacharge",
    when(
        (col("situationfamiliale").isin("Marie(e)", "Divorce", "En Couple")) & 
        (col("nbenfantacharge").isNull() | (col("nbenfantacharge") == -1)),
        2
    ).otherwise(col("nbenfantacharge"))
)

#Fusionner F/F�minin/Femme en F
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "Femme", "F"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "F�minin", "F"))

#Fusionner H/M/Masculin/Hommme en H
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "Masculin", "H"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "M", "H"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "Homme", "H"))

#Remplacer les null/? par N/D
df_client = df_client.withColumn("sexe", when(col("sexe") == None, "N/D").otherwise(col("sexe")))
df_client = df_client.withColumn("sexe",when(trim(col("sexe")) == "", "N/D").otherwise(col("sexe")))
df_client = df_client.withColumn("sexe",regexp_replace(col("sexe"), r"\?", "N/D"))

#Supprimer les N/D si les proportions le permettent
df_client= df_client.filter(col("sexe") != "N/D")

df_nulls = df_client.filter(col("deuxiemevoiture").isNull())
# Ajouter une colonne aléatoire et assigner 'true' ou 'false' selon les proportions
df_nulls_replaced = df_nulls.withColumn(
    "deuxiemevoiture",
    when(rand() < 0.13, True).otherwise(False)
)
# Filtrer les lignes sans 'null' dans 'deuxiemevoiture'
df_non_nulls = df_client.filter(col("deuxiemevoiture").isNotNull())
# Combiner les deux DataFrames
df_client = df_non_nulls.union(df_nulls_replaced)

#Suppression des doublons
df_client= df_client.dropDuplicates(["immatriculation"])

#Changer le type de la colonne
df_immat = df_immat.withColumn("occasion", df_immat["occasion"].cast(BooleanType()))

#Fusion des tables
df_client_immat= df_client.join(df_immat, on= "immatriculation")

#Supression des doublons
df_client_immat= df_client_immat.dropDuplicates(["immatriculation"])

# Ajout de la colonne 'categorie' avec des critères précis
df_client_immat= df_client_immat.withColumn(
    "categorie",
    when(
        (col("longueur") == "courte") & (col("puissance") < 100) & (col("prix") < 20000),
        "citadine economique"
    )
    .when(
        (col("longueur") == "courte") & (col("puissance") >= 100) & (col("prix") >= 20000),
        "citadine standard"
    )
    .when(
        (col("longueur").isin("moyenne", "longue")) & (col("nbplaces") >= 5) & (col("prix") < 35000),
        "familiale"
    )
    .when(
        (col("longueur").isin("longue", "tres longue")) & (col("nbplaces") >= 5) & (col("prix") >= 35000),
        "SUV/Crossover"
    )
    .when(
        (col("puissance") >= 200) & (col("prix") >= 40000),
        "sportive"
    )
    .when(
        (col("prix") >= 50000),
        "luxe"
    )
    .otherwise("autre")
)

# Nom de la table cible
table_name = "clients_immatriculations"

table_exists = spark._jsparkSession.catalog().tableExists("concessionnaire", table_name)

if not table_exists:
    # Créer la table si elle n'existe pas
    print('Création et enregistrement de la table "clients_immatriculations"')
    df_client_immat.write.saveAsTable(table_name)
else:
    # Si la table existe, remplacer ou insérer dans la table
    print('Enregistrement dans la table "clients_immatriculations"')
    df_client_immat.write.mode("overwrite").saveAsTable(table_name)