import os
from datetime import datetime

import requests

def get_paris_realtime_bicycle_data():
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    
    response = requests.request("GET", url)
    
    serialize_data(response.text, "paris_realtime_bicycle_data.json")

def get_nantes_realtime_bicycle_data():
    """
    Récupère les données des vélos en libre-service pour Nantes et les stocke localement.
    """
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    
    response = requests.request("GET", url)
    
    if response.status_code == 200:
        serialize_data(response.text, "nantes_realtime_bicycle_data.json")
    else:
        print(f"Erreur lors de la récupération des données de Nantes : {response.status_code}")

def get_communes_data():
    """
    Récupère les données des communes via l'API Open Data Communes et les stocke localement.
    """
    url = "https://geo.api.gouv.fr/communes?format=json&geometry=centre"
    response = requests.get(url)
    
    if response.status_code == 200:
        serialize_data(response.text, "communes_data.json")
        print("Les données des communes ont été récupérées et sauvegardées avec succès.")
    else:
        print(f"Erreur lors de la récupération des données des communes : {response.status_code}")

def serialize_data(raw_json: str, file_name: str):

    today_date = datetime.now().strftime("%Y-%m-%d")
    
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)
