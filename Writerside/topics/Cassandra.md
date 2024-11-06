# Cassandra

Connexion au container
```bash
docker exec -it cassandra cqlsh
```

Création de la base de données
```bash
CREATE KEYSPACE IF NOT EXISTS co2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

utiliser la base de donnée
```bash
USE co2;
```

```sql
CREATE TABLE IF NOT EXISTS crit_air (
    id INT,
    marque_modele TEXT PRIMARY KEY,
    bonus_malus TEXT,
    rejets_co2 FLOAT,
    cout_energie TEXT
);
```

Import des donnée CO2.csv
```SQL
COPY crit_air (id, marque_modele, bonus_malus, rejets_co2, cout_energie) FROM '/import/CO2.csv' WITH HEADER = TRUE;
```

## Commandes utiles
```SQL
DROP TABLE IF EXISTS co2.crit_air;
```