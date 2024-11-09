# HIVE

Connexion au container
```bash
docker exec -it hive-server bash
```

Lancer Hive
```bash
hive
```

Création de la table Clients (Table interne)
```SQL
CREATE TABLE clients (
    age INT,
    sexe STRING,
    taux INT,
    situationFamiliale STRING,
    nbEnfantAcharge INT,
    deuxiemeVoiture BOOLEAN,
    immatriculation STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

Creation d'une table externe
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS concessionnaire.immatriculations_ext (
    immatriculation STRING,
    marque STRING,
    nom STRING,
    puissance INT,
    longueur STRING,
    nbPlaces INT,
    nbPortes INT,
    couleur STRING,
    occasion STRING,
    prix INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/immatriculations_ext';
```

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS crit_air_ext (
    marque_modele STRING,
    bonus_malus STRING,
    rejets_co2 FLOAT,
    cout_energie STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/crit_air_ext';
```
Création de la table externe catalogue_ext
```SQL
CREATE EXTERNAL TABLE IF NOT EXISTS catalogue_ext (
    marque STRING,
    nom STRING,
    puissance INT,
    longueur STRING,
    nbPlaces INT,
    nbPortes INT,
    couleur STRING,
    occasion BOOLEAN,
    prix FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/concessionnaire/catalogue'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

```

Création de la table externe Marketing_ext
```SQL
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_ext (
    age INT,
    sexe STRING,
    taux FLOAT,
    situationFamiliale STRING,
    nbEnfantAcharge INT,
    deuxiemeVoiture BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/concessionnaire/marketing'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);
```

Créate une base de données
```sql
CREATE DATABASE IF NOT EXISTS concessionnaire;
```

Chargement des données dans la table
```SQL
LOAD DATA INPATH '/user/concessionnaire/clients.csv' INTO TABLE clients;
```

Vider la table clients
```SQL
TRUNCATE TABLE clients;
```

Supprimer une table
```sql
DROP TABLE IF EXISTS concessionnaire.immatriculations_ext;
```

```sql
DROP TABLE IF EXISTS concessionnaire.crit_air_ext;
```

## Commandes utiles
Lister les jar chargé
```SQL
LIST JARS;
```
Démarrer Beeline
```bash
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
```