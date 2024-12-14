import duckdb

# Connexion à la base de données
con = duckdb.connect("data/duckdb/mobility_analysis.duckdb")

# Requête 1 : Nb d'emplacements disponibles de vélos dans une ville
query_1 = """
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes');
"""
result_1 = con.execute(query_1).fetchdf()
print("Nb d'emplacements disponibles de vélos dans une ville")
print(result_1)

# Requête 2 : Nb de vélos disponibles en moyenne dans chaque station
query_2 = """
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
"""
result_2 = con.execute(query_2).fetchdf()
print("Nb de vélos disponibles en moyenne dans chaque station")
print(result_2)
