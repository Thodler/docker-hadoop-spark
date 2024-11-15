#!/bin/bash

echo "===== Initialisation de la suppression des BDD externes ====="

echo "----- Suppression sur Cassandra -----"
# Attendre que Cassandra soit prêt
echo "Attente de Cassandra..."
until echo "SELECT now() FROM system.local;" | docker exec -i cassandra cqlsh; do
  sleep 5
done

echo "Cassandra est prêt. Suppression des données dans la table 'crit_air'."
docker exec -i cassandra bash -c "cqlsh -e 'TRUNCATE co2.crit_air;'"

echo "----- Suppression sur MongoDB -----"

# Attendre que MongoDB soit prêt
echo "Attente de MongoDB..."
until docker exec mongodb mongosh --eval "db.adminCommand('ping')"; do
  sleep 5
done

echo "MongoDB est prêt. Suppression de la collection 'immatriculations'."
docker exec -i mongodb bash -c "mongosh concessionnaire --eval 'db.immatriculations.drop();'"

echo "===== Suppression terminée ====="
sleep 10
