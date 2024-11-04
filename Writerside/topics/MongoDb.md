# MongoDb

Connection au shell
```bash
docker exec -it mongodb bash
```

Import des Immatriculations
```bash
mongoimport --db concessionnaire --collection immatriculations --file /data/import/Immatriculations.csv --type csv --headerline
```

