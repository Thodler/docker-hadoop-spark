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