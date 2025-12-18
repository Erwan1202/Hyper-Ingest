# Pour lancer le micro-service Flask interrogeant DuckDB :

1. Installez les dépendances nécessaires :

   pip install flask duckdb

2. Vérifiez que votre base DuckDB existe à l'emplacement build/Release/hyper_ingest.duckdb (ou modifiez DUCKDB_PATH dans api_service.py).

3. Lancez le service :

   python api_service.py

4. Interrogez l'API depuis votre C++ ou via un navigateur :

   http://localhost:5000/datasets/?q=dechets verts

Le service renverra les résultats au format JSON compatible avec l'API data.gouv.fr.
