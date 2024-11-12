# Spark

[SheetCheat](https://images.datacamp.com/image/upload/v1676302905/Marketing/Blog/PySpark_SQL_Cheat_Sheet.pdf)

Executer CLI Spark dans le container
```sql
/spark/bin/pyspark --master spark://spark-master:7077
```

Script d'importation 'Immatriculation' MongoDb.
```bash
/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master spark://spark-master:7077 spark-app/mongo_import.py
```

Aggregation de catalogue et Co2
```bash
/spark/bin/spark-submit --master spark://spark-master:7077 spark-app/aggregate_catalogue_co2.py
```

spark/bin/spark-submit --conf spark.sql.hive.metastore.uris=thrift://hive-metastore:9083 spark-app/aggregate_catalogue_co2.py

Vider le cache des script
```bash
rm -rf ~/.ivy2/cache
rm -rf ~/.ivy2/jars
```