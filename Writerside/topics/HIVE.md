# HIVE

Connexion au container
```bash
docker exec -it hive-server bash
```

Lancer Hive
```bash
hive
```

Création de la table Clients
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

Chargement des données dans la table
```SQL
LOAD DATA INPATH '/user/concessionnaire/clients.csv' INTO TABLE clients;
```

Vider la table clients
```SQL
TRUNCATE TABLE clients;
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