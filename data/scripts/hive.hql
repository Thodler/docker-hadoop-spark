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


CREATE EXTERNAL TABLE IF NOT EXISTS catalogue_ext (
    marque STRING,
    Nom STRING,
    Puissance STRING,
    Longueur STRING,
    nbPlaces STRING,
    nbPortes STRING,
    Couleur STRING,
    Occasion STRING,
    Prix STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/concessionnaire';