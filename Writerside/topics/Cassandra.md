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
CREATE TABLE voitures_co2 (
    marque TEXT,
    modele TEXT,
    bonus_malus TEXT,
    rejet_co2 INT,
    cout_energie DECIMAL,
    PRIMARY KEY ((marque), modele)
);
```