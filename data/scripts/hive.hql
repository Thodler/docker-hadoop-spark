CREATE DATABASE IF NOT EXISTS concessionnaire;

USE concessionnaire;

CREATE TABLE IF NOT EXISTS clients (
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
STORED AS TEXTFILE
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

LOAD DATA INPATH '/user/hive/warehouse/concessionnaire.db/clients.csv' INTO TABLE clients;

CREATE EXTERNAL TABLE IF NOT EXISTS immatriculations_ext (
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

CREATE EXTERNAL TABLE IF NOT EXISTS crit_air_ext (
    marque_modele STRING,
    bonus_malus STRING,
    rejets_co2 FLOAT,
    cout_energie STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/crit_air_ext';


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
STORED AS TEXTFILE LOCATION '/user/hive/warehouse/concessionnaire.db/catalogue_ext'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);

CREATE EXTERNAL TABLE IF NOT EXISTS marketing_ext (
    age INT,
    sexe STRING,
    taux FLOAT,
    situationFamiliale STRING,
    nbEnfantAcharge INT,
    deuxiemeVoiture BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/hive/warehouse/concessionnaire.db/marketing_ext'
TBLPROPERTIES (
    "skip.header.line.count"="1"
);