import duckdb
import json
import pandas as pd
from datetime import datetime, date

# Définir la date actuelle et les codes des villes
# Code pour la ville de Paris et Nantes
# Ces codes seront utilisés pour identifier les données spécifiques à chaque ville
today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
NANTES_CITY_CODE = 2

def create_consolidate_tables():
    """
    Crée les tables dans la base de données DuckDB à partir des requêtes SQL définies dans un fichier externe.
    Le fichier contient les schémas des tables nécessaires au pipeline de consolidation.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
    for statement in statements.split(";"):
        if statement.strip():  # Ignorer les instructions vides
            print(statement)
            con.execute(statement)

def fetch_city_code_mapping():
    """
    Récupère la correspondance entre les codes des villes et leurs noms dans la table CONSOLIDATE_CITY.
    Cette correspondance est utilisée pour mapper les codes de ville aux données qui n'en possèdent pas directement.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    query = """
    SELECT id AS city_code, name AS city_name
    FROM CONSOLIDATE_CITY
    """
    city_code_mapping = con.query(query).to_df()
    return city_code_mapping

def consolidate_paris_station_data():
    """
    Consolide les données des stations de vélos de Paris.
    Les données sont chargées depuis un fichier JSON et transformées pour correspondre au format attendu dans la base de données.
    """
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"PARIS-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]].copy()

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code",
        "capacity":"capacitty"
    }, inplace=True)

    return paris_station_data_df

def consolidate_nantes_station_data():
    """
    Consolide les données des stations de vélos de Nantes.
    Les données sont chargées depuis un fichier JSON et transformées pour correspondre au format attendu dans la base de données.
    """
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"NANTES-{x}")
    nantes_raw_data_df["created_date"] = date.today()

    city_code_mapping = fetch_city_code_mapping()
    city_code_dict = dict(zip(city_code_mapping["city_name"].str.lower(), city_code_mapping["city_code"]))

    nantes_raw_data_df["city_code"] = nantes_raw_data_df["contract_name"].str.lower().map(city_code_dict)

    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]].copy()

    nantes_station_data_df.rename(columns={
        "number": "code",
        "name": "name",
        "contract_name": "city_name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "bike_stands": "capacitty"
    }, inplace=True)

    return nantes_station_data_df

def consolidate_station_data():
    """
    Combine les données des stations de vélos de Paris et Nantes dans une table unique.
    Les données consolidées sont insérées dans la table CONSOLIDATE_STATION de la base de données.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    paris_station_data_df = consolidate_paris_station_data()
    nantes_station_data_df = consolidate_nantes_station_data()

    combined_station_data_df = pd.concat([paris_station_data_df, nantes_station_data_df], ignore_index=True)

    # Vérification des colonnes pour correspondre au schéma
    expected_columns = [
        "id", "code", "name", "city_name", "city_code",
        "address", "longitude", "latitude", "status", "created_date", "capacitty"
    ]
    combined_station_data_df = combined_station_data_df[expected_columns].copy()

    print("Schéma attendu :", expected_columns)
    print("Colonnes dans le DataFrame :", combined_station_data_df.columns.tolist())
    print("Exemple de données :", combined_station_data_df.head())

    # Charger le DataFrame dans une table temporaire
    con.execute("CREATE OR REPLACE TEMP TABLE temp_station_data AS SELECT * FROM combined_station_data_df")

    # Insérer les données dans la table principale
    con.execute("""
    INSERT OR REPLACE INTO CONSOLIDATE_STATION
    SELECT * FROM temp_station_data
    """)
    print("Consolidated station data for Paris and Nantes.")
    


def consolidate_paris_station_statement_data():
    """
    Consolide les relevés des stations de vélos pour Paris.
    Les données consolidées sont préparées pour être insérées dans la table CONSOLIDATE_STATION_STATEMENT.
    """
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"PARIS-{x}")
    paris_raw_data_df["created_date"] = date.today()

    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]].copy()

    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date"
    }, inplace=True)

    return paris_station_statement_data_df

def consolidate_nantes_station_statement_data():
    """
    Consolide les relevés des stations de vélos pour Nantes.
    Les données consolidées sont préparées pour être insérées dans la table CONSOLIDATE_STATION_STATEMENT.
    """
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"NANTES-{x}")
    nantes_raw_data_df["created_date"] = date.today()

    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]].copy()

    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date"
    }, inplace=True)

    return nantes_station_statement_data_df

def consolidate_station_statement_data():
    """
    Combine les relevés des stations de vélos de Paris et Nantes dans une table unique.
    Les données consolidées sont insérées dans la table CONSOLIDATE_STATION_STATEMENT de la base de données.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    paris_station_statement_data_df = consolidate_paris_station_statement_data()
    nantes_station_statement_data_df = consolidate_nantes_station_statement_data()

    combined_station_statement_data_df = pd.concat([paris_station_statement_data_df, nantes_station_statement_data_df], ignore_index=True)

    combined_station_statement_data_df = combined_station_statement_data_df[[
    "station_id",
    "bicycle_docks_available",
    "bicycle_available",
    "last_statement_date",
    "created_date", 
    ]].copy()

     # Enregistrer temporairement le DataFrame comme table DuckDB
    con.execute("CREATE OR REPLACE TEMP TABLE temp_station_statement_data AS SELECT * FROM combined_station_statement_data_df")
                
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM temp_station_statement_data;")
    print("Consolidated station statement data for Paris and Nantes.")

def consolidate_city_data():
    """
    Consolide les données des villes en utilisant l'API Open Data Communes.
    Les données consolidées sont insérées dans la table CONSOLIDATE_CITY de la base de données.
    """
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)

    communes_raw_data_df = pd.json_normalize(data)
    communes_raw_data_df["created_date"] = date.today()

    city_data_df = communes_raw_data_df[[
        "code",
        "nom",
        "population",
        "created_date"
    ]].copy()

    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    print("Consolidated city data from Open Data Communes API.")
