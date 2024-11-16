from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, when, trim, split, regexp_replace, round, lower, col, encode, count, levenshtein, row_number, broadcast, coalesce, udf, lit

import re
### FUNCTIONS ###

# Définir la fonction pour extraire le modèle
def extract_model(modele_str):
    words = modele_str.strip().split()
    model_words = []
    # Liste de mots à arrêter
    stopwords = set([
        'berline',
        'e-tense',
        'hyb.',
        'hybrid',
        'hybride',
        'e-hybrid',
        'moteur',
        'active',
        'sportback',
        'xdrive',
        'moteur',
        's-phev',
        'crossback',
        'electrique',
        'cabrio',
        'coupe',
        '4matic',
        'roadster',
        'electric',
        'plug-in',
        'se',
        'phev',
        'sw',
        'tourer'
    ])
    for word in words:
        word_clean = word.lower().strip('()')
        if word_clean in stopwords:
            break
        elif re.match(r'^\d+\.\d+(ch|kw|t)?$', word_clean):  # nombres décimaux
            break
        elif re.match(r'^\d+(ch|kw|t)$', word_clean):  # nombres avec suffixes
            break
        else:
            model_words.append(word)
    return ' '.join(model_words)
# Cette fonction supprime tout après un mot choisi
def extract_model_after(modele_str):
    words = modele_str.strip().split()
    model_words = []
    # Liste de mots à arrêter
    stopwords = set([
        'forfour',
        'ev400',
        'c 300',
        't8',
        't6',
        't5'
    ])

    for word in words:
        word_clean = word.lower().strip('()')
        model_words.append(word)  # Ajouter le mot au modèle

        # Si le mot est un stopword, arrêter après l'avoir ajouté
        if word_clean in stopwords:
            break
        # Si le mot correspond aux formats numériques, arrêter sans l'ajouter
        elif re.match(r'^\d+\.\d+(ch|kw|t)?$', word_clean) or re.match(r'^\d+(ch|kw|t)$', word_clean):
            model_words.pop()  # Enlever le dernier mot ajouté (le numéro) et arrêter
            break

    return ' '.join(model_words)

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

# co2 dispose de marque et modele dans la meme colonne
df_co2 = df_co2.withColumn("marque", split(df_co2["marque_modele"], " ", 2).getItem(0))
df_co2 = df_co2.withColumn("modele", split(df_co2["marque_modele"], " ", 2).getItem(1))
df_co2 = df_co2.drop('marque_modele')

# La colonne **nom** du catalogue est renommée en **modele**
df_catalogue = df_catalogue.withColumnRenamed("nom", "modele")

# Les colonnes 'marque' et 'modele' dans les 2 tableaux n’ont pas la même casse.
df_co2 = df_co2.withColumn("marque", lower(trim(col("marque"))))
df_catalogue = df_catalogue.withColumn("marque", lower(trim(col("marque"))))

df_co2 = df_co2.withColumn("modele", lower(trim(col("modele"))))
df_catalogue = df_catalogue.withColumn("modele", lower(trim(col("modele"))))

# Nettoyage des données spécifiques
df_co2 = df_co2.withColumn("bonus_malus", split(trim(df_co2["bonus_malus"]), "€").getItem(0))
df_co2 = df_co2.withColumn("cout_energie", split(trim(df_co2["cout_energie"]), "€").getItem(0))
df_co2 = df_co2.withColumn("bonus_malus", regexp_replace(trim(df_co2["bonus_malus"]), "[^0-9-]", "").cast("float"))
df_catalogue = df_catalogue.withColumn("longueur", regexp_replace(col("longueur"), "�", "e"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "ã©", "e"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "ã", "a"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "copper", "cooper"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "e- tense", "e-tense"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "5p", ""))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "eq ", ""))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "120 ah", ""))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "nouvelle", ""))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "nissan", ""))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "tfsi e", "tfsie"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "rover range", "range"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "rover lwb", "rover"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "rover sport", "rover"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "rover swb", "rover"))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "xdrive", ""))
df_co2 = df_co2.withColumn("modele", regexp_replace("modele", "combi", ""))

df_co2 = df_co2.withColumn(
    "cout_energie",
    regexp_replace(col("cout_energie"), "\u00A0", "")
)
df_co2 = df_co2.withColumn("cout_energie", col("cout_energie").cast("float"))

marques_correctes_df = df_co2.select('marque').distinct().alias('marques_correctes')
marques_catalogue_df = df_catalogue.select('marque').distinct().alias('marques_catalogue')

df_cross = marques_catalogue_df.crossJoin(broadcast(marques_correctes_df))

df_cross = df_cross.withColumn('distance', levenshtein(col('marques_catalogue.marque'), col('marques_correctes.marque')))

window = Window.partitionBy('marques_catalogue.marque').orderBy(col('distance'))
df_min_distance = df_cross.withColumn('rn', row_number().over(window)).filter(col('rn') == 1)

marque_mapping = df_min_distance.select(
    col('marques_catalogue.marque').alias('marque_catalogue'),
    col('marques_correctes.marque').alias('marque_correcte'),
    'distance'
).filter(col('distance') <= 2)

df_catalogue_corrected = df_catalogue.join(
    marque_mapping,
    df_catalogue.marque == marque_mapping.marque_catalogue,
    how='left'
)

df_catalogue = df_catalogue_corrected.withColumn(
    'marque',
    coalesce(col('marque_correcte'), col('marque'))
).drop('marque_catalogue', 'marque_correcte', 'distance')

# Calcul des moyennes par marque
df_co2_agg_bonus_malus_marque = df_co2.groupby("marque").agg(round(avg("bonus_malus")).alias("moyenne_bonus_malus_marque"))
df_co2_agg_rejets_co2_marque = df_co2.groupby("marque").agg(round(avg("rejets_co2")).alias("moyenne_rejets_co2_marque"))
df_co2_agg_cout_energie_marque = df_co2.groupby("marque").agg(round(avg("cout_energie")).alias("moyenne_cout_energie_marque"))

extract_model_udf = udf(extract_model, StringType())
extract_model_after_udf = udf(extract_model_after, StringType())

df_co2 = df_co2.withColumn('modele', extract_model_udf(df_co2.modele))
df_co2 = df_co2.withColumn('modele', extract_model_after_udf(df_co2.modele))

# Calcul des moyennes par modèle
df_catalogue_result_model = df_catalogue.withColumn('modele', split(df_catalogue['modele'], ' ').getItem(0))
df_catalogue_co2_model = df_co2.withColumn('modele', split(df_co2['modele'], ' ').getItem(0))

df_co2_agg_bonus_malus_modele = df_catalogue_co2_model.groupby("modele").agg(round(avg("bonus_malus")).alias("moyenne_bonus_malus_modele"))
df_co2_agg_rejets_co2_modele = df_catalogue_co2_model.groupby("modele").agg(round(avg("rejets_co2")).alias("moyenne_rejets_co2_modele"))
df_co2_agg_cout_energie_modele = df_catalogue_co2_model.groupby("modele").agg(round(avg("cout_energie")).alias("moyenne_cout_energie_modele"))

# Calcul des moyennes globales
moyenne_bonus_malus_global = df_co2_agg_bonus_malus_modele.agg(round(avg('moyenne_bonus_malus_modele'), 2)).collect()[0][0]
moyenne_rejets_co2_global = df_co2_agg_rejets_co2_modele.agg(round(avg('moyenne_rejets_co2_modele'), 2)).collect()[0][0]
moyenne_cout_energie_global = df_co2_agg_cout_energie_modele.agg(round(avg('moyenne_cout_energie_modele'), 2)).collect()[0][0]

# Jointure des données du catalogue avec celles de CO2
df_co2 = df_co2.withColumnRenamed("modele", "modele_co2")
df_catalogue_with_co2 = df_catalogue.join(df_co2, on="marque", how="left")

# Joindre les moyennes par marque
df_catalogue_with_co2 = df_catalogue_with_co2.join(df_co2_agg_bonus_malus_marque, on="marque", how="left")
df_catalogue_with_co2 = df_catalogue_with_co2.join(df_co2_agg_rejets_co2_marque, on="marque", how="left")
df_catalogue_with_co2 = df_catalogue_with_co2.join(df_co2_agg_cout_energie_marque, on="marque", how="left")

# Remplir les valeurs manquantes selon les consignes
df_catalogue_with_moyennes = df_catalogue_with_co2.withColumn(
    'bonus_malus',
    when(
        col('bonus_malus').isNull() & col('moyenne_bonus_malus_marque').isNotNull(),
        col('moyenne_bonus_malus_marque')
    ).when(
        col('bonus_malus').isNull() & col('moyenne_bonus_malus_marque').isNull(),
        lit(moyenne_bonus_malus_global)
    ).otherwise(col('bonus_malus'))
).withColumn(
    'rejets_co2',
    when(
        col('rejets_co2').isNull() & col('moyenne_rejets_co2_marque').isNotNull(),
        col('moyenne_rejets_co2_marque')
    ).when(
        col('rejets_co2').isNull() & col('moyenne_rejets_co2_marque').isNull(),
        lit(moyenne_rejets_co2_global)
    ).otherwise(col('rejets_co2'))
).withColumn(
    'cout_energie',
    when(
        col('cout_energie').isNull() & col('moyenne_cout_energie_marque').isNotNull(),
        col('moyenne_cout_energie_marque')
    ).when(
        col('cout_energie').isNull() & col('moyenne_cout_energie_marque').isNull(),
        lit(moyenne_cout_energie_global)
    ).otherwise(col('cout_energie'))
)

# Nettoyer les colonnes inutiles
df_catalogue_with_moyennes = df_catalogue_with_moyennes.drop("modele_co2", "moyenne_bonus_malus_marque", "moyenne_rejets_co2_marque", "moyenne_cout_energie_marque")


# Ajout de la colonne 'categorie' avec des critères précis
df_catalogue_with_moyennes = df_catalogue_with_moyennes.withColumn(
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
        "suv/crossover"
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

df_catalogue_with_moyennes = df_catalogue_with_moyennes.groupBy("marque", "modele", "longueur", "nbplaces", "nbportes", "categorie") \
    .agg(
        round(avg("bonus_malus")).alias("bonus_malus"),
        round(avg("rejets_co2")).alias("rejets_co2"),
        round(avg("cout_energie")).alias("cout_energie"),
        round(avg("puissance")).alias("puissance")
)

df_catalogue_with_moyennes = df_catalogue_with_moyennes.dropDuplicates()


# Nom de la table cible
table_name = "catalogue"

table_exists = spark._jsparkSession.catalog().tableExists("concessionnaire", table_name)

if not table_exists:
    # Créer la table si elle n'existe pas
    print('Création et enregistrement de la table "catalogue"')
    df_catalogue_with_moyennes.write.saveAsTable(table_name)
else:
    # Si la table existe, remplacer ou insérer dans la table
    print('Enregistrement dans la table "catalogue"')
    df_catalogue_with_moyennes.write.mode("overwrite").saveAsTable(table_name)
