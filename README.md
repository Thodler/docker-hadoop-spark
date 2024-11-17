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
Ce script met en place tout le systeme des base avec le chargement de donnée et l'analyse.
Pour simuler un autre traitement utiliser executer le script <code>./del_all_tables.sh</code>, 
il supprime Le contenue de **MongoDB** et **Cassandra**. Et executer a nouveau <code>./init_import.sh</code> pour les 
rechargers et actualiser les tables **Hive**

## Outils
[Hadoop & HDFS](http://localhost:9870/dfshealth.html#tab-overview)

[Spark Worker](http://localhost:8080/)

### Jupiter
[Interface](http://localhost:8888/#/)

Pour accéder a l'interface jupiter vous devez recupérer un token:
```bash
 docker logs notebook
```
Le token est afficher dans l'URL montré dans les logs.
