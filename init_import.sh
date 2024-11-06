#!/bin/bash

echo "===== Initialisation de la stack Docker ====="

echo "----- Importation sur HDFS -----"

# Attendre que le NameNode soit prêt
echo "Attente du NameNode HDFS..."
until docker exec namenode hdfs dfsadmin -safemode get | grep -q "Safe mode is OFF"; do
  echo "Le HDFS est en mode sécurisé, attente de 5 secondes..."
  sleep 5
done

echo "HDFS est prêt, importation des fichiers."

docker exec namenode bash -c "hdfs dfs -mkdir -p /user/concessionnaire"
docker exec namenode bash -c "hdfs dfs -put /data/import/Clients_1.csv /user/concessionnaire/clients.csv"
docker exec namenode bash -c "hdfs dfs -put /data/import/Catalogue.csv /user/concessionnaire/catalogue.csv"
docker exec namenode bash -c "hdfs dfs -put /data/import/Marketing.csv /user/concessionnaire/marketing.csv"

echo "----- Importation sur Cassandra -----"
# Attendre que Cassandra soit prêt
echo "Attente de Cassandra..."
until echo "SELECT now() FROM system.local;" | docker exec -i cassandra cqlsh; do
  sleep 5
done

echo "Cassandra est prêt, exécution du script CQL d'import."
docker exec -i cassandra bash -c "cqlsh -f '/import/scripts/cassandra.cql'"

echo "----- Importation sur MongoDB -----"

# Attendre que MongoDB soit prêt
echo "Attente de MongoDB..."
until docker exec mongodb mongosh --eval "db.adminCommand('ping')"; do
  sleep 5
done

echo "MongoDB est prêt, importation des données dans la collection 'immatriculations'."
docker exec -i mongodb bash -c "mongoimport --db concessionnaire --collection immatriculations --file /data/import/Immatriculations.csv --type csv --headerline"

echo "----- Importation sur HiveServer2 -----"

# Attendre que HiveServer2 soit prêt
echo "Attente de HiveServer2..."
until docker exec hive-server nc -z localhost 10000; do
  echo "HiveServer2 n'est pas encore prêt, attente de 5 secondes..."
  sleep 5
done

docker exec -i hive-server bash -c "hive -f '/import/hive.hql'"

echo "===== Importation terminée ====="
sleep 10