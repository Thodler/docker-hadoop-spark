# Spark

Script d'importation 'Immatriculation' MongoDb.
```bash
spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1   spark-app/mongo_test.py
```

Vider le cache des script
```bash
rm -rf ~/.ivy2/cache
rm -rf ~/.ivy2/jars
```