
import requests
import json
from transformers import pipeline

# ----------------- CONFIGURATION -----------------
# URL de l'API de data.gouv.fr pour les datasets
API_URL = "https://www.data.gouv.fr/api/1/datasets/"
# Nombre de datasets à traiter (pour l'exemple)
DATASET_COUNT = 50
# Fichier de sortie
OUTPUT_FILE = "data_enriched.json"
# Modèle pour l'extraction de mots-clés (NER - Named Entity Recognition)
# Ce modèle est efficace pour extraire des entités (lieux, organisations, etc.)
# qui sont souvent d'excellents mots-clés contextuels.
KEYWORD_EXTRACTOR_MODEL = "Jean-Baptiste/camembert-ner"
# Seuil de confiance pour les mots-clés extraits
SCORE_THRESHOLD = 0.9
# -----------------------------------------------

def fetch_datasets(url, count):
    """Récupère les datasets depuis l'API de data.gouv.fr."""
    print(f"Récupération de {count} datasets depuis {url}...")
    try:
        params = {'page_size': count}
        response = requests.get(url, params=params)
        response.raise_for_status()  # Lève une exception en cas d'erreur HTTP
        print("Récupération réussie.")
        return response.json().get('data', [])
    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return None

def enrich_data(datasets, keyword_extractor):
    """Enrichit les datasets avec des mots-clés contextuels."""
    enriched_datasets = []
    print(f"Début de l'enrichissement pour {len(datasets)} datasets...")
    for i, dataset in enumerate(datasets):
        title = dataset.get('title', '')
        description = dataset.get('description', '')
        
        # Concaténer le titre et la description pour avoir plus de contexte
        text_to_analyze = f"{title}. {description}"
        
        if not text_to_analyze.strip():
            continue

        # Extraire les entités (mots-clés)
        try:
            keywords_raw = keyword_extractor(text_to_analyze)
            
            # Filtrer et nettoyer les mots-clés
            enriched_keywords = set()
            for entity in keywords_raw:
                # On ne garde que les entités avec un score élevé
                if entity['score'] > SCORE_THRESHOLD:
                    # On nettoie le mot (ex: "d'Aix-en-Provence" -> "Aix-en-Provence")
                    clean_word = entity['word']
                    if "##" in clean_word: continue # Ignorer les sous-mots de CamemBERT
                    if "'" in clean_word:
                        clean_word = clean_word.split("'")[1]

                    enriched_keywords.add(clean_word)

            # Créer le nouvel objet dataset enrichi
            new_dataset = dataset.copy()
            # On ajoute les mots-clés existants avec les nouveaux
            existing_tags = {tag['name'] for tag in dataset.get('tags', [])}
            all_keywords = list(enriched_keywords.union(existing_tags))
            new_dataset['enriched_keywords'] = all_keywords
            
            enriched_datasets.append(new_dataset)
            print(f"  - Dataset {i+1}/{len(datasets)} enrichi. Mots-clés ajoutés: {list(enriched_keywords)}")

        except Exception as e:
            print(f"Erreur lors de l'analyse du dataset '{title}': {e}")
            # Ajouter le dataset original même en cas d'erreur d'analyse
            enriched_datasets.append(dataset)

    print("Enrichissement terminé.")
    return enriched_datasets

def save_to_json(data, filename):
    """Sauvegarde les données dans un fichier JSON."""
    print(f"Sauvegarde des données dans {filename}...")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("Sauvegarde réussie.")
    except IOError as e:
        print(f"Erreur lors de la sauvegarde du fichier : {e}")

def main():
    """Fonction principale du script."""
    print("Lancement du script d'enrichissement de données.")
    
    # 1. Initialiser le pipeline d'extraction de mots-clés
    try:
        print(f"Chargement du modèle '{KEYWORD_EXTRACTOR_MODEL}'...")
        # Utiliser device=0 si vous avez un GPU compatible CUDA
        keyword_extractor = pipeline('ner', model=KEYWORD_EXTRACTOR_MODEL, aggregation_strategy="simple")
        print("Modèle chargé.")
    except Exception as e:
        print(f"Impossible de charger le modèle. Assurez-vous d'avoir installé les dépendances.")
        print(f"Erreur: {e}")
        return

    # 2. Récupérer les données
    datasets = fetch_datasets(API_URL, DATASET_COUNT)
    if not datasets:
        return
        
    # 3. Enrichir les données
    enriched_datasets = enrich_data(datasets, keyword_extractor)
    
    # 4. Sauvegarder les données enrichies
    save_to_json(enriched_datasets, OUTPUT_FILE)
    
    print("Script terminé.")

if __name__ == "__main__":
    main()
