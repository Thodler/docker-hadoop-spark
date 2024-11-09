# Architecture du datalake

## Localisation des informations
| MongoDB         | HBase | HDFS      | HIVE   |
|-----------------|-------|-----------|--------|
| Immatriculation | Co2   | Catalogue | Client |
| -               | -     | Marketing |        |

```mermaid
classDiagram  
    class Immatriculations{
        -String immaticulation
        -String marque
        -String nom
        -int puissance
        -String longueur
        -int nbPlaces
        -int nbPortes
        -String couleur
        -bool occasion
        -int prix
    }
    
    class Catalogue{
        -String marque
        -String nom
        -int puissance
        -List longueur
        -int nbPlaces
        -int nbPortes
        -List couleur
        -bool occasion
        -int prix
    }
    
    class Client{
        -int age
        -List sexe
        -int taux
        -List situationFamiliale
        -int nbEnfantAcharge
        -bool deuxiemeVoiture
        -String immatriculation
    }
    
    class Marketing{
        -int age
        -List sexe
        -int taux
        -List situationFamiliale
        -int nbEnfantAcharge
        -bool deuxiemeVoiture
    }
    
    class Co2{
        -String marqueModel
        -String bonusMalus
        -int rejetsCo2
        -String coutEnergie
    }
```

```mermaid
graph BT
    NAMENODE[Namenode\nPorts: 9870, 9000] --> HDFS[HDFS]
    DATANODE[Datanode\nPort: 9864] --> HDFS
    RESOURCEMANAGER[ResourceManager] --> NAMENODE
    RESOURCEMANAGER --> DATANODE
    SPARK[Spark] --> RESOURCEMANAGER
    SPARK --> HDFS

    HIVE[Hive\nDatabase] --> HDFS
    MONGODB[MongoDB\nDatabase] --> SPARK
    CASSANDRA[Cassandra\nDatabase] --> SPARK

    CATALOGUE[Table catalogue] --> HIVE
    CO2_EXT[Table CO2] --> HIVE
```