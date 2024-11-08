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
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/concessionnaire/clients.csv' INTO TABLE clients;

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

CREATE EXTERNAL TABLE IF NOT EXISTS concessionnaire.co2_ext (
    marque_modele STRING,
    bonus_malus STRING,
    cout_energie STRING,
    rejets_co2 INT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/concessionnaire.db/co2_ext';