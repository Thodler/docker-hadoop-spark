from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("AggregateCatalogueCo2") \
    .config('hive.metastore.warehouse.dir', '/user/hive/warehouse') \
    .config('hive.metastore.uris', 'thrift://hive-metastore:9083') \
    .enableHiveSupport()\
    .getOrCreate()

spark.catalog.clearCache()

spark.sql("SHOW DATABASES").show()
spark.sql("USE concessionnaire")
spark.sql("SHOW TABLES").show()
spark.sql("SELECT * FROM marketing_ext LIMIT 5").show()
# co2_df = spark.sql("SELECT * FROM concessionnaire.crit_air_ext")
# catalogue_df = spark.sql("SELECT * FROM concessionnaire.clients")
# #
# #
# co2_df.show()
# catalogue_df.show()
