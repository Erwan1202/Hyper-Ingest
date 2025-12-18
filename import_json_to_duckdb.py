import duckdb
import json
import os

DUCKDB_PATH = "build/Release/hyper_ingest.duckdb"
JSON_PATH = "data_enriched.json"
TABLE_NAME = "datasets_enriched"

# Charger les données JSON
with open(JSON_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

# Connexion à DuckDB
con = duckdb.connect(DUCKDB_PATH)

# Créer la table (drop si existe déjà)
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")


# Trouver toutes les clés présentes dans tous les objets
all_keys = set()
for row in data:
    all_keys.update(row.keys())
all_keys = list(all_keys)

# Déterminer le type de chaque colonne (si au moins un int/float, sinon VARCHAR)
col_types = {}
for k in all_keys:
    col_types[k] = 'VARCHAR'  # par défaut
for row in data:
    for k in all_keys:
        v = row.get(k, None)
        if isinstance(v, int):
            col_types[k] = 'INTEGER'
        elif isinstance(v, float):
            col_types[k] = 'DOUBLE'
        elif isinstance(v, (list, dict)):
            col_types[k] = 'VARCHAR'
# Générer la requête de création de table
columns = [f'{k} {col_types[k]}' for k in all_keys]
col_str = ", ".join(columns)
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
con.execute(f"CREATE TABLE {TABLE_NAME} ({col_str})")

# Insérer les données
for row in data:
    values = []
    for k in all_keys:
        v = row.get(k, None)
        if isinstance(v, (list, dict)):
            values.append(json.dumps(v, ensure_ascii=False))
        else:
            values.append(v)
    placeholders = ", ".join(["?"] * len(all_keys))
    con.execute(f"INSERT INTO {TABLE_NAME} ({', '.join(all_keys)}) VALUES ({placeholders})", values)

print(f"Import terminé ! Table créée : {TABLE_NAME}")
con.close()
