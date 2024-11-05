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