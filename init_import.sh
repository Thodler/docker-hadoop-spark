#!/bin/bash

echo "===== Initialisation de la stack Docker ====="

# Attendre que Cassandra soit prêt
echo "Attente de Cassandra..."
until echo "SELECT now() FROM system.local;" | docker exec -i cassandra cqlsh; do
  sleep 5
done

echo "Cassandra est prêt, exécution du script CQL d'import."
docker exec -i cassandra bash -c "cqlsh -f '/import/scripts/cassandra.cql'"



sleep 10