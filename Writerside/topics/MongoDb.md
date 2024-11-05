# MongoDb

Connexion au container
```bash
docker exec -it mongodb bash
```

Import des 'Immatriculations'
```bash
mongoimport --db concessionnaire --collection immatriculations --file /data/import/Immatriculations.csv --type csv --headerline
```
## SheetCheat
Connection à MongoDB à partir du container
```bash
mongosh
```
Changer de BDD
```bash
# use database
use concessionnaire
```
Afficher un résultat de la collection
```bash
# db.la_collection.findOne()
db.immatriculations.findOne()
```

