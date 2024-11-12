# Projet concessionnaire (Mise en situation)

## Prerequis
- Docker

## Mise en place

_Les commandes sont à executer à la racine du projet._

Téléchargement et installation de la stack technique
```bash
docker-compose up -d
```
Déploiment des datas
```bash
./init_import.sh
```
## Liens du projet

### Interfaces
[Hadoop & HDFS](http://localhost:9870/dfshealth.html#tab-overview)
[Zeppelin](http://localhost:9099/#/)
[Spark Worker](http://localhost:8080/)