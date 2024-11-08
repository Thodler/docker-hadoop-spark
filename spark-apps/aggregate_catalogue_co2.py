from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("AggregateCatalogueCo2") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")\
    .enableHiveSupport()\
    .getOrCreate()


spark.sql("SHOW DATABASES").show()
# co2_df = spark.sql("SELECT * FROM concessionnaire.crit_air_ext")
# catalogue_df = spark.sql("SELECT * FROM concessionnaire.clients")
# #
# #
# co2_df.show()
# catalogue_df.show()
