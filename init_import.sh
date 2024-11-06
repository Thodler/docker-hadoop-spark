#!/bin/bash

echo "===== Initialisation de la stack Docker ====="

# Attendre que Cassandra soit prêt
echo "Attente de Cassandra..."
until echo "SELECT now() FROM system.local;" | docker exec -i cassandra cqlsh; do
  sleep 5
done

echo "Cassandra est prêt, exécution du script CQL d'import."
docker exec -i cassandra bash -c "cqlsh -f '/import/scripts/cassandra.cql'"

# Attendre que MongoDB soit prêt
echo "Attente de MongoDB..."
until docker exec mongodb mongosh --eval "db.adminCommand('ping')"; do
  sleep 5
done

echo "MongoDB est prêt, importation des données dans la collection 'immatriculations'."
docker exec -i mongodb bash -c "mongoimport --db concessionnaire --collection immatriculations --file /data/import/Immatriculations.csv --type csv --headerline"

echo "===== Initialisation terminée ====="

sleep 10