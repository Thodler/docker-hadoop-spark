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