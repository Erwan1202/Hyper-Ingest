#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <vector>
#include <sstream>
#include "core/RingBuffer.hpp"
#include "core/ThreadPool.hpp"
#include "data/StorageEngine.hpp"
#include "search/SearchService.hpp"

std::atomic<bool> g_running{true};
std::atomic<size_t> g_bytes_ingested{0};
std::atomic<size_t> g_records_processed{0};

void consumerWorker(civic::RingBuffer<std::string>& buffer, civic::StorageEngine& storage) {
    auto con = storage.createConnection();
    std::string payload;
    while (g_running) {
        if (buffer.pop(payload)) {
            g_bytes_ingested += payload.size();
            storage.ingest(*con, payload); 
            g_records_processed++;
        } else {
            std::this_thread::yield();
        }
    }
}

void mockProducer(civic::RingBuffer<std::string>& queue) {
    std::string mock_json = R"({
        "slideshow": {
            "author": "HighFreq Bot", 
            "title": "Benchmark Data",
            "date": "2025"
        }
    })";

    while(g_running) {
        if(!queue.push(mock_json)) {
            std::this_thread::yield(); 
        }
    }
}

void afficherThematiques() {
    std::cout << "\n=== THEMATIQUES DISPONIBLES ===\n";
    auto themes = civic::SearchService::getThematiques();
    int i = 0;
    for (const auto& [theme, nom] : themes) {
        std::cout << "  [" << i++ << "] " << nom << "\n";
    }
}

civic::Thematique parseThematique(const std::string& input) {
    static const std::vector<civic::Thematique> thematiques = {
        civic::Thematique::ADMINISTRATION,
        civic::Thematique::ECONOMIE,
        civic::Thematique::TRANSPORTS,
        civic::Thematique::SANTE,
        civic::Thematique::ENVIRONNEMENT,
        civic::Thematique::EDUCATION,
        civic::Thematique::CULTURE,
        civic::Thematique::LOGEMENT,
        civic::Thematique::AGRICULTURE,
        civic::Thematique::ENERGIE,
        civic::Thematique::SECURITE,
        civic::Thematique::SOCIAL,
        civic::Thematique::TOURISME,
        civic::Thematique::NUMERIQUE,
        civic::Thematique::TOUTES
    };
    
    try {
        int index = std::stoi(input);
        if (index >= 0 && index < static_cast<int>(thematiques.size())) {
            return thematiques[index];
        }
    } catch (...) {}
    
    return civic::Thematique::TOUTES;
}

civic::Territoire parseTerritoire(const std::string& input) {
    if (input == "1" || input == "national") return civic::Territoire::NATIONAL;
    if (input == "2" || input == "regional") return civic::Territoire::REGIONAL;
    if (input == "3" || input == "departemental") return civic::Territoire::DEPARTEMENTAL;
    if (input == "4" || input == "communal") return civic::Territoire::COMMUNAL;
    if (input == "5" || input == "epci") return civic::Territoire::EPCI;
    return civic::Territoire::TOUS;
}

civic::SourceType parseSource(const std::string& input) {
    if (input == "1" || input == "insee") return civic::SourceType::INSEE;
    if (input == "2" || input == "ministere") return civic::SourceType::MINISTERE;
    if (input == "3" || input == "spd") return civic::SourceType::COLLECTIVITE_SPD;
    if (input == "4" || input == "operateur") return civic::SourceType::OPERATEUR_NATIONAL;
    if (input == "5" || input == "etablissement") return civic::SourceType::ETABLISSEMENT_PUBLIC;
    return civic::SourceType::TOUTES;
}

void afficherResultats(const civic::ResultatRecherche& resultats) {
    std::cout << "\n=== RESULTATS DE RECHERCHE ===\n";
    std::cout << "Total: " << resultats.totalResultats << " jeux de données\n";
    std::cout << "Page: " << resultats.pageCourante << "/" << resultats.totalPages << "\n";
    std::cout << "Temps: " << resultats.tempsRecherche.count() << "ms\n";
    std::cout << "-----------------------------------\n\n";
    
    int i = 1;
    for (const auto& jeu : resultats.jeux) {
        std::cout << "[" << i++ << "] " << jeu.titre << "\n";
        std::cout << "    Organisation: " << jeu.organisation;
        if (jeu.organisationCertifiee) std::cout << " ✓ SPD";
        std::cout << "\n";
        std::cout << "    Ressources: " << jeu.ressources.size() << "\n";
        
        std::cout << "    Formats: ";
        for (const auto& res : jeu.ressources) {
            std::cout << civic::SearchService::formatVersMimeType(res.format) << " ";
        }
        std::cout << "\n\n";
    }
}

civic::ResultatRecherche lancerRecherche(
    civic::SearchService& searchService,
    civic::RingBuffer<std::string>& queue,
    const civic::CriteresRecherche& criteres
) {
    std::cout << "\n[SEARCH] Lancement de la recherche...\n";
    
    auto resultats = searchService.rechercher(criteres);
    afficherResultats(resultats);
    
    return resultats;
}

void ingererDataset(
    civic::SearchService& searchService,
    civic::RingBuffer<std::string>& queue,
    const civic::JeuDeDonnees& dataset
) {
    std::cout << "\n[INGEST] Ingestion du dataset: " << dataset.titre << "\n";
    
    for (const auto& ressource : dataset.ressources) {
        auto verification = searchService.verifierRessource(ressource.url);
        
        if (verification.disponible) {
            std::cout << "  ✓ Ressource disponible: " << ressource.titre << "\n";
            
            std::ostringstream json;
            json << "{"
                 << "\"type\":\"datagouv_resource\","
                 << "\"dataset_id\":\"" << dataset.id << "\","
                 << "\"resource_id\":\"" << ressource.id << "\","
                 << "\"titre\":\"" << ressource.titre << "\","
                 << "\"url\":\"" << ressource.url << "\","
                 << "\"format\":\"" << civic::SearchService::formatVersMimeType(ressource.format) << "\","
                 << "\"taille\":" << ressource.taille
                 << "}";
            
            if (!queue.push(json.str())) {
                std::cout << "  ! Buffer plein, attente...\n";
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                queue.push(json.str());
            }
            
            g_bytes_ingested += json.str().size();
            g_records_processed++;
        } else {
            std::cout << "  ✗ Ressource indisponible (HTTP " << verification.httpStatus << "): " 
                      << ressource.titre << "\n";
        }
    }
}

void modeRechercheInteractif(
    civic::SearchService& searchService,
    civic::RingBuffer<std::string>& queue
) {
    std::cout << "\n";
    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║          HYPER-INGEST - RECHERCHE DATA.GOUV.FR               ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    
    afficherThematiques();
    
    std::cout << "\n=== TERRITOIRES ===\n";
    std::cout << "  [0] Tous  [1] National  [2] Régional  [3] Départemental  [4] Communal  [5] EPCI\n";
    
    std::cout << "\n=== SOURCES ===\n";
    std::cout << "  [0] Toutes  [1] INSEE  [2] Ministère  [3] SPD  [4] Opérateur  [5] Établissement\n";
    
    std::cout << "\n--- Entrez vos critères (laisser vide pour ignorer) ---\n";
    
    civic::CriteresBuilder builder;
    std::string input;
    
    std::cout << "Recherche textuelle: ";
    std::getline(std::cin, input);
    if (!input.empty()) {
        builder.requete(input);
    }
    
    std::cout << "Thématique [0-14]: ";
    std::getline(std::cin, input);
    if (!input.empty()) {
        builder.thematique(parseThematique(input));
    }
    
    std::cout << "Territoire [0-5]: ";
    std::getline(std::cin, input);
    if (!input.empty()) {
        builder.territoire(parseTerritoire(input));
    }
    
    std::cout << "Source [0-5]: ";
    std::getline(std::cin, input);
    if (!input.empty()) {
        builder.source(parseSource(input));
    }
    
    std::cout << "Uniquement sources certifiées SPD? [o/n]: ";
    std::getline(std::cin, input);
    if (input == "o" || input == "O" || input == "oui") {
        builder.certifieesUniquement(true);
    }
    
    std::cout << "Nombre de résultats par page [20]: ";
    std::getline(std::cin, input);
    if (!input.empty()) {
        try {
            builder.parPage(std::stoi(input));
        } catch (...) {}
    }
    
    auto criteres = builder.build();
    auto resultats = lancerRecherche(searchService, queue, criteres);
    
    if (!resultats.jeux.empty()) {
        std::cout << "\nIngérer un dataset? Entrez le numéro [1-" << resultats.jeux.size() << "] ou 'q' pour quitter: ";
        std::getline(std::cin, input);
        
        if (input != "q" && input != "Q") {
            try {
                int choix = std::stoi(input) - 1;
                if (choix >= 0 && choix < static_cast<int>(resultats.jeux.size())) {
                    ingererDataset(searchService, queue, resultats.jeux[choix]);
                }
            } catch (...) {
                std::cout << "Choix invalide.\n";
            }
        }
    }
}

civic::ResultatRecherche rechercherAvecFiltres(
    civic::SearchService& searchService,
    const std::string& requete,
    civic::Thematique thematique = civic::Thematique::TOUTES,
    civic::Territoire territoire = civic::Territoire::TOUS,
    civic::SourceType source = civic::SourceType::TOUTES,
    bool certifieesUniquement = false,
    int parPage = 20
) {
    auto criteres = civic::CriteresBuilder()
        .requete(requete)
        .thematique(thematique)
        .territoire(territoire)
        .source(source)
        .certifieesUniquement(certifieesUniquement)
        .parPage(parPage)
        .formatsStricts({civic::FormatFichier::CSV, civic::FormatFichier::JSON})
        .verifierDisponibilite(true)
        .build();
    
    return searchService.rechercher(criteres);
}

void monitoringLoop() {
    auto last_time = std::chrono::steady_clock::now();
    size_t last_bytes = 0;
    size_t last_records = 0;

    std::cout << "\n[ SYSTEM STARTED : IN-MEMORY MODE ]\n" << std::endl;
    std::cout << std::left << std::setw(15) << "TIME" 
              << std::setw(15) << "NET (MB/s)" 
              << std::setw(15) << "DB (Rec/s)" 
              << std::setw(15) << "TOTAL" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        auto now = std::chrono::steady_clock::now();
        size_t current_bytes = g_bytes_ingested.load();
        size_t current_records = g_records_processed.load();

        double elapsed = std::chrono::duration<double>(now - last_time).count();
        double mb_s = (double)(current_bytes - last_bytes) / (1024 * 1024) / elapsed;
        double rec_s = (double)(current_records - last_records) / elapsed;

        std::cout << "\r" 
                  << std::left << std::setw(15) << "[RUNNING]" 
                  << std::fixed << std::setprecision(2) << std::setw(15) << mb_s 
                  << std::setw(15) << rec_s 
                  << std::setw(15) << current_records << std::flush;

        last_time = now;
        last_bytes = current_bytes;
        last_records = current_records;
    }
}

int main(int argc, char* argv[]) {

    bool modeRecherche = false;
    bool modeDemo = false;
    std::string requeteDirecte;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--search" || arg == "-s") {
            modeRecherche = true;
        } else if (arg == "--demo" || arg == "-d") {
            modeDemo = true;
        } else if (arg == "--query" || arg == "-q") {
            if (i + 1 < argc) {
                requeteDirecte = argv[++i];
                modeRecherche = true;
            }
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "Usage: " << argv[0] << " [OPTIONS]\n\n";
            std::cout << "Options:\n";
            std::cout << "  -s, --search       Mode recherche interactif\n";
            std::cout << "  -q, --query TEXT   Recherche directe avec le texte spécifié\n";
            std::cout << "  -d, --demo         Mode démo (recherche exemple)\n";
            std::cout << "  -h, --help         Affiche cette aide\n";
            std::cout << "\nExemples:\n";
            std::cout << "  " << argv[0] << " --search\n";
            std::cout << "  " << argv[0] << " --query \"population communes\"\n";
            std::cout << "  " << argv[0] << " --demo\n";
            return 0;
        }
    }

    civic::StorageEngine storage(":memory:");
    civic::RingBuffer<std::string> queue(8192);
    civic::SearchService searchService;

    if (modeRecherche || modeDemo) {
        
        if (modeDemo) {
            std::cout << "\n[DEMO] Recherche: 'INSEE population' - sources certifiées uniquement\n";
            
            auto resultats = rechercherAvecFiltres(
                searchService,
                "INSEE population",
                civic::Thematique::TOUTES,
                civic::Territoire::TOUS,
                civic::SourceType::TOUTES,
                true,
                10
            );
            
            if (!resultats.jeux.empty()) {
                std::cout << "\n[AUTO-INGEST] Ingestion du premier résultat...\n";
                ingererDataset(searchService, queue, resultats.jeux[0]);
            }
            
        } else if (!requeteDirecte.empty()) {
            std::cout << "\n[SEARCH] Recherche: '" << requeteDirecte << "'\n";
            auto resultats = rechercherAvecFiltres(searchService, requeteDirecte);
            
            if (!resultats.jeux.empty()) {
                std::string input;
                std::cout << "\nIngérer un dataset? [1-" << resultats.jeux.size() << "/n]: ";
                std::getline(std::cin, input);
                
                if (input != "n" && input != "N") {
                    try {
                        int choix = std::stoi(input) - 1;
                        if (choix >= 0 && choix < static_cast<int>(resultats.jeux.size())) {
                            ingererDataset(searchService, queue, resultats.jeux[choix]);
                        }
                    } catch (...) {}
                }
            }
            
        } else {
            modeRechercheInteractif(searchService, queue);
        }
        
        return 0;
    }

    unsigned int num_workers = std::max(1u, std::thread::hardware_concurrency() - 2);
    civic::ThreadPool consumerPool(num_workers);
    consumerPool.setTask([&queue, &storage](){ 
        consumerWorker(queue, storage); 
    });
    
    std::cout << "[INIT] Workers: " << num_workers << " | Storage: RAM (Zero-Latency)" << std::endl;
    std::cout << "[INFO] Utilisez --search pour le mode recherche ou --help pour l'aide" << std::endl;

    std::thread producerThread(mockProducer, std::ref(queue));

    monitoringLoop();

    if (producerThread.joinable()) producerThread.join();
    return 0;
}
