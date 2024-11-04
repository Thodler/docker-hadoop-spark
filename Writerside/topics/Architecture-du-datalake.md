# Architecture du datalake

## Localisation des informations
| MongoDB         | Oracle NoSQL | HDFS      | HIVE   |
|-----------------|--------------|-----------|--------|
| Immatriculation | Co2          | Catalogue | Client |
| -               | -            | Marketing |        |

```mermaid
classDiagram  
    class Immatriculation{
        -String immaticulation
        -String marque
        -String nom
        -integer puissance
        -List longueur
        -int nbPlaces
        -int nbPortes
        -List couleur
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
    
    
```