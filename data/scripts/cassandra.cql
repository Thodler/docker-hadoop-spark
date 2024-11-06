CREATE KEYSPACE IF NOT EXISTS co2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE co2;

CREATE TABLE IF NOT EXISTS crit_air (
    id INT,
    marque_modele TEXT PRIMARY KEY,
    bonus_malus TEXT,
    rejets_co2 FLOAT,
    cout_energie TEXT
);

COPY crit_air (id, marque_modele, bonus_malus, rejets_co2, cout_energie) FROM '/import/CO2.csv' WITH HEADER = TRUE;