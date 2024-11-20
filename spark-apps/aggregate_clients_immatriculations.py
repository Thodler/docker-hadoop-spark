from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, FloatType
from pyspark.sql.functions import trim, lower, upper, col, count, regexp_replace, max, min, when,avg, round, rand

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

#Suppression de la 1ère ligne de la table clients
df_client = df_client.filter(df_client['immatriculation'] != 'immatriculation')

#Normalisation des casses
df_immat = df_immat.withColumn("marque", lower(trim(col("marque"))))
df_immat = df_immat.withColumn("modele", lower(trim(col("modele"))))
df_immat = df_immat.withColumn("immatriculation", upper(trim(col("immatriculation"))))

df_client = df_client.withColumn("situationfamiliale", lower(trim(col("situationfamiliale"))))
df_client = df_client.withColumn("immatriculation", upper(trim(col("immatriculation"))))

#Correction du symbole "�" dans la colonne "longueur"
df_immat = df_immat.withColumn("longueur", regexp_replace(col("longueur"), "�", "e"))
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "�", "e"))

#Mettre à null les valeurs inférieures à 18 ans.
df_client = df_client.withColumn("age", when(col("age") == -1, None).otherwise(col("age")))
df_client = df_client.withColumn("age", when(col("age") == 0, None).otherwise(col("age")))

#Calculer la médiane et remplacer les nulls par la médiane.
mediane = df_client.approxQuantile("age", [0.5], 0.01)[0]
df_client = df_client.withColumn("age", when(col("age").isNull(), mediane).otherwise(col("age")))

#Fusionner seul/seule en celibataire
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "seule", "celibataire"))
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "seul", "celibataire"))
df_client = df_client.withColumn("situationfamiliale", regexp_replace(col("situationfamiliale"), "divorcee", "divorce(e)"))

# Remplacer "n/d" par null dans la colonne situationfamiliale
df_client = df_client.withColumn(
    "situationfamiliale",
    when(col("situationfamiliale") == None, "n/d").otherwise(col("situationfamiliale"))
)
df_client = df_client.withColumn(
    "situationfamiliale",
    when(trim(col("situationfamiliale")) == "", "n/d").otherwise(col("situationfamiliale"))
)
# Remplacer "?" par "n/d" dans la colonne situationfamiliale
df_client = df_client.withColumn(
    "situationfamiliale",
    regexp_replace(col("situationfamiliale"), r"\?", "n/d")
)
#Supprimer les n/d car les proportions le permettent
df_client= df_client.filter(col("situationfamiliale") != "n/d")

# Remplacer les nulls de taux
mediane_taux = df_client.approxQuantile("taux", [0.5], 0.01)[0]
df_client = df_client.withColumn("taux", when(col("taux").isNull(), mediane).otherwise(col("taux")))

#Correction des valeurs hors domaine (Création d'une colonne "taux_eligible")
df_client = df_client.withColumn(
    "taux_eligible",
    when((col("taux") >= 544) & (col("taux") <= 74185), True).otherwise(False)
)

#Fusionner F/F�minin/Femme en F
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "Femme", "F"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "F�minin", "F"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "F", "F"))

#Fusionner H/M/Masculin/Hommme en H
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "Masculin", "M"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "Homme", "M"))
df_client = df_client.withColumn("sexe", regexp_replace(col("sexe"), "H", "M"))

#Remplacer les null/? par n/d
df_client = df_client.withColumn("sexe", when(col("sexe") == None, "n/d").otherwise(col("sexe")))
df_client = df_client.withColumn("sexe",when(trim(col("sexe")) == "", "n/d").otherwise(col("sexe")))
df_client = df_client.withColumn("sexe",when(trim(col("sexe")) == "N/D", "n/d").otherwise(col("sexe")))
df_client = df_client.withColumn("sexe",regexp_replace(col("sexe"), r"\?", "n/d"))

#Supprimer les n/d si les proportions le permettent
df_client= df_client.filter(col("sexe") != "n/d")

df_client = df_client.withColumn(
    "nbenfantacharge",
    when(
        (col("situationfamiliale") == "celibataire") & (col("nbenfantacharge").isNull() | (col("nbenfantacharge") == -1)),
        0
    ).otherwise(col("nbenfantacharge"))
)

df_client = df_client.withColumn(
    "nbenfantacharge",
    when(
        (col("situationfamiliale").isin("marie(e)", "divorce(e)", "en couple")) & 
        (col("nbenfantacharge").isNull() | (col("nbenfantacharge") == -1)),
        2
    ).otherwise(col("nbenfantacharge"))
)

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

from pyspark.sql.functions import col, when

# Ajout de la colonne 'categorie' avec des critères précis et exhaustifs
df_client_immat = df_client_immat.withColumn(
    "categorie",
    when(
        (col("longueur") == "courte") & (col("nbplaces") <= 4) & (col("prix") < 20000),
        "citadine"
    )
    .when(
        (col("longueur") == "moyenne") & (col("nbplaces") >= 4) & (col("prix") < 25000),
        "compacte"
    )
    .when(
        (col("longueur").isin("longue", "moyenne")) & (col("nbplaces") >= 5) & (col("prix") >= 30000),
        "berline"
    )
    .when(
        (col("longueur") == "longue") & (col("nbplaces") >= 5),
        "break"
    )
    .when(
        (col("longueur").isin("longue", "tres longue")) & (col("nbplaces") >= 5) & (col("prix") >= 35000),
        "SUV"
    )
    .when(
        (col("longueur").isin("longue", "tres longue")) & (col("nbplaces") >= 6),
        "monospace"
    )
    .otherwise("citadine")
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