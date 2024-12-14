
# DataEngineering-Project

Ce projet consiste à créer un pipeline ETL pour analyser les données des stations de vélos en libre-service dans plusieurs villes françaises. Le pipeline est conçu pour ingérer, transformer, et agréger des données provenant de différentes sources (API Open Data), puis les stocker dans une base de données locale DuckDB pour des analyses ultérieures.

## **Installation**

### **1. Prérequis**
- **Python** : Assurez-vous que Python 3.12 ou une version ultérieure est installé.
- **Pip** : Le gestionnaire de paquets Python doit être installé.
- **DuckDB** : Une base de données légère utilisée pour ce projet.

### **2. Cloner le repository**
Clonez ce projet sur votre machine locale :

```bash
git clone https://github.com/ChancellaLITOKO/DataEngineering-Project.git
cd DataEngineering-Project
python -m venv .venv #Créez un environnement virtuel pour isoler les dépendances
.venv\Scripts\activate (Windows) ou
source .venv/bin/activate (Linux, Mac)
pip install -r requirements.txt
```
Pour executer le pipeline ETL complet il faudrait lancer le fichier principal main.py
```bash
python src/main.py
```
## **EXPLICATION LOGIQUE DE LA PIPELINE**
Le pipeline suit une approche ETL structurée en trois grandes étapes : Ingestion, Consolidation, et Agrégation. Ces étapes permettent de transformer des données brutes provenant des API en un modèle de données prêt pour l'analyse.

### **1. Ingestion des données**
À cette étape, nous avons extrait les données des villes de Paris, de Nantes et des communes françaises grâce à leurs API respectives : l'API Paris Velib' Temps Réel (https://opendata.paris.fr), l'API Open Data Nantes (https://data.nantesmetropole.fr), et l'API Geo API Gouv (https://geo.api.gouv.fr/communes). Ces données incluent la disponibilité des vélos, les informations sur les stations, ainsi que les détails des communes (nom, code INSEE, population). Les données extraites ont été sauvegardées localement au format JSON dans le répertoire data/raw_data/YYYY-MM-DD/. 

### **2. Consolidation des données**

À cette étape, les données brutes des villes et des communes sont nettoyées, enrichies, et structurées avant d'être chargées dans la base de données DuckDB. Les fichiers JSON sont transformés en DataFrames pandas, les colonnes inutiles sont supprimées, et les données manquantes complétées. Des colonnes comme city_code sont ajoutées pour relier les stations à leurs villes, et les données des stations de Nantes sont enrichies avec des informations issues de la table CONSOLIDATE_CITY. Enfin, les données consolidées sont chargées dans les tables CONSOLIDATE_CITY (données des villes), CONSOLIDATE_STATION (descriptif des stations), et CONSOLIDATE_STATION_STATEMENT (relevés de vélos).

### **3. Aggregation des données**

Les données consolidées sont transformées en un modèle dimensionnel pour faciliter l'analyse. Les informations sont réparties dans trois types de tables : DIM_CITY (caractéristiques des villes), DIM_STATION (informations sur les stations), et FACT_STATION_STATEMENT (données de disponibilité des vélos). Les données sont agrégées pour inclure des mesures comme la capacité totale des stations ou le nombre de vélos disponibles, et seules les dernières mises à jour sont conservées.







