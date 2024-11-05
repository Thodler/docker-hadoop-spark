# HDFS

Connexion au container
```bash
docker exec -it namenode bash
```

Copie des fichiers sur HDFS
```bash
hdfs dfs -mkdir /user/concessionnaire
```
```bash
hdfs dfs -put /data/import/Clients_1.csv /user/concessionnaire/clients.csv
```
```bash
hdfs dfs -put /data/import/Catalogue.csv /user/concessionnaire/catalogue.csv
```
```bash
hdfs dfs -put /data/import/Marketing.csv /user/concessionnaire/marketing.csv
```