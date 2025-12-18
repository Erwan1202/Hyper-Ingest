from flask import Flask, request, jsonify
import duckdb
import os

# Configuration
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "build/Release/hyper_ingest.duckdb")

app = Flask(__name__)

def query_datasets(q=None):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    # Adapter la requête selon la structure de votre table
    table = "datasets_enriched"
    if q:
        # Recherche plein texte sur tous les champs textuels pertinents
        sql = f"""
        SELECT * FROM {table}
        WHERE (title IS NOT NULL AND lower(title) LIKE lower(?))
           OR (description IS NOT NULL AND lower(description) LIKE lower(?))
           OR (tags IS NOT NULL AND lower(tags) LIKE lower(?))
           OR (enriched_keywords IS NOT NULL AND lower(enriched_keywords) LIKE lower(?))
           OR (metrics IS NOT NULL AND lower(metrics) LIKE lower(?))
        LIMIT 50
        """
        like_q = f"%{q}%"
        results = con.execute(sql, (like_q, like_q, like_q, like_q, like_q)).fetchall()
    else:
        sql = f"SELECT * FROM {table} LIMIT 50"
        results = con.execute(sql).fetchall()
    # Récupérer les noms de colonnes
    columns = [desc[0] for desc in con.description]
    con.close()
    # Transformer en liste de dicts
    return [dict(zip(columns, row)) for row in results]

@app.route("/datasets/")
def datasets():
    q = request.args.get("q")
    results = query_datasets(q)
    # Adapter le format de réponse pour imiter data.gouv.fr
    response = {
        "data": results,
        "total": len(results)
    }
    return jsonify(response)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
