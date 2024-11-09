from pyspark.sql import SparkSession

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

df_catalogue.show()
df_co2.show()
