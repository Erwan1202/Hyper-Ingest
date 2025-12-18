#include "search/SearchService.hpp"
#include <simdjson.h>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <ctime>
#include <iostream>
#include <regex>
#include <fstream>

#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>

namespace civic {

    namespace beast = boost::beast;
    namespace http = beast::http;
    namespace net = boost::asio;
    namespace ssl = net::ssl;
    using tcp = net::ip::tcp;

    namespace {
        std::string urlEncode(const std::string& value) {
            std::ostringstream escaped;
            escaped.fill('0');
            escaped << std::hex;
            
            for (char c : value) {
                if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.' || c == '~') {
                    escaped << c;
                } else {
                    escaped << '%' << std::setw(2) << static_cast<int>(static_cast<unsigned char>(c));
                }
            }
            return escaped.str();
        }

        std::chrono::system_clock::time_point parseISODate(const std::string& dateStr) {
            std::tm tm = {};
            std::istringstream ss(dateStr);
            ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
            if (ss.fail()) {
                return std::chrono::system_clock::now();
            }
            return std::chrono::system_clock::from_time_t(std::mktime(&tm));
        }

        std::string formatISODate(std::chrono::system_clock::time_point tp) {
            auto time_t = std::chrono::system_clock::to_time_t(tp);
            std::tm tm = *std::gmtime(&time_t);
            std::ostringstream ss;
            ss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
            return ss.str();
        }

        std::tuple<std::string, std::string, std::string> parseUrl(const std::string& url) {
            std::regex urlRegex(R"((https?)://([^/:]+)(?::(\d+))?(/.*)?)", std::regex::icase);
            std::smatch match;
            
            if (std::regex_match(url, match, urlRegex)) {
                std::string scheme = match[1].str();
                std::string host = match[2].str();
                std::string port = match[3].str();
                std::string path = match[4].str();
                
                if (port.empty()) {
                    port = (scheme == "https") ? "443" : "80";
                }
                if (path.empty()) {
                    path = "/";
                }
                
                return {host, port, path};
            }
            return {"", "", ""};
        }

        // Normalise une chaîne : minuscules, suppression accents, trim
        std::string normaliserTexte(const std::string& texte) {
            std::string resultat;
            resultat.reserve(texte.size());
            
            // Table de conversion des caractères accentués UTF-8
            static const std::vector<std::pair<std::string, std::string>> accents = {
                {"é", "e"}, {"è", "e"}, {"ê", "e"}, {"ë", "e"},
                {"à", "a"}, {"â", "a"}, {"ä", "a"},
                {"ù", "u"}, {"û", "u"}, {"ü", "u"},
                {"î", "i"}, {"ï", "i"},
                {"ô", "o"}, {"ö", "o"},
                {"ç", "c"},
                {"É", "e"}, {"È", "e"}, {"Ê", "e"}, {"Ë", "e"},
                {"À", "a"}, {"Â", "a"}, {"Ä", "a"},
                {"Ù", "u"}, {"Û", "u"}, {"Ü", "u"},
                {"Î", "i"}, {"Ï", "i"},
                {"Ô", "o"}, {"Ö", "o"},
                {"Ç", "c"}
            };
            
            std::string temp = texte;
            for (const auto& [accent, replacement] : accents) {
                size_t pos = 0;
                while ((pos = temp.find(accent, pos)) != std::string::npos) {
                    temp.replace(pos, accent.length(), replacement);
                    pos += replacement.length();
                }
            }
            
            for (char c : temp) {
                if (std::isalnum(static_cast<unsigned char>(c)) || c == ' ' || c == '-') {
                    resultat += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                }
            }
            
            // Trim
            size_t start = resultat.find_first_not_of(" ");
            size_t end = resultat.find_last_not_of(" ");
            if (start == std::string::npos) return "";
            return resultat.substr(start, end - start + 1);
        }

        // Dictionnaire de synonymes pour expansion de requête
        const std::unordered_map<std::string, std::vector<std::string>>& getSynonymes() {
            static const std::unordered_map<std::string, std::vector<std::string>> synonymes = {
                // Transports
                {"transport", {"mobilite", "deplacement", "circulation", "trafic"}},
                {"velo", {"cyclable", "piste-cyclable", "bicyclette", "velocipede"}},
                {"bus", {"autobus", "transport-commun", "ligne-bus"}},
                {"train", {"sncf", "ferroviaire", "rail", "gare", "ter", "tgv"}},
                {"voiture", {"automobile", "vehicule", "parking", "stationnement"}},
                {"metro", {"metropolitain", "rer", "tramway", "tram"}},
                
                // Environnement
                {"environnement", {"ecologie", "nature", "biodiversite", "climat"}},
                {"pollution", {"qualite-air", "emission", "co2", "particules"}},
                {"dechets", {"ordures", "recyclage", "tri", "collecte"}},
                {"eau", {"assainissement", "potable", "cours-eau", "riviere"}},
                {"energie", {"electricite", "gaz", "renouvelable", "solaire", "eolien"}},
                
                // Santé
                {"sante", {"medical", "hopital", "medecin", "soins"}},
                {"hopital", {"chu", "clinique", "urgences", "etablissement-sante"}},
                {"medecin", {"generaliste", "specialiste", "praticien", "docteur"}},
                {"pharmacie", {"officine", "medicament"}},
                
                // Éducation
                {"education", {"enseignement", "scolaire", "formation"}},
                {"ecole", {"primaire", "maternelle", "elementaire", "etablissement-scolaire"}},
                {"college", {"secondaire", "collegien"}},
                {"lycee", {"lyceen", "baccalaureat"}},
                {"universite", {"faculte", "etudiant", "superieur", "campus"}},
                
                // Économie
                {"economie", {"entreprise", "commerce", "emploi", "activite"}},
                {"emploi", {"travail", "chomage", "offre-emploi", "recrutement"}},
                {"entreprise", {"societe", "siret", "siren", "etablissement"}},
                {"commerce", {"magasin", "boutique", "commercant"}},
                
                // Logement
                {"logement", {"habitat", "immobilier", "residence", "habitation"}},
                {"hlm", {"social", "logement-social", "bailleur"}},
                
                // Administration
                {"mairie", {"commune", "municipal", "hotel-ville"}},
                {"prefecture", {"departement", "sous-prefecture"}},
                {"region", {"conseil-regional", "collectivite"}},
                
                // Culture
                {"culture", {"musee", "bibliotheque", "theatre", "patrimoine"}},
                {"sport", {"equipement-sportif", "stade", "gymnase", "piscine"}},
                
                // Sécurité
                {"securite", {"police", "gendarmerie", "pompier", "secours"}},
                {"accident", {"sinistre", "incident", "accidentologie"}},
                
                // Agriculture
                {"agriculture", {"agricole", "exploitation", "ferme", "elevage"}},
                {"bio", {"biologique", "agriculture-biologique", "label"}}
            };
            return synonymes;
        }

        // Normalise et nettoie la requête (sans ajouter de mots - l'API fait un AND implicite)
        std::string expandreRequete(const std::string& requete) {
            std::string requeteNorm = normaliserTexte(requete);
            
            // Retourner la requête normalisée (accents supprimés, minuscules)
            // On n'ajoute PAS de synonymes car l'API data.gouv fait un AND entre tous les mots
            return requeteNorm;
        }
    }

    // Retourne les tags associés à une thématique
    std::vector<std::string> SearchService::getTagsThematique(Thematique theme) {
        static const std::unordered_map<Thematique, std::vector<std::string>> tagsParThematique = {
            {Thematique::ADMINISTRATION, {"administration", "service-public", "collectivite", "mairie", "demarche"}},
            {Thematique::ECONOMIE, {"economie", "entreprise", "emploi", "commerce", "industrie", "pib", "siret"}},
            {Thematique::TRANSPORTS, {"transport", "mobilite", "deplacement", "circulation", "velo", "bus", "train", "metro"}},
            {Thematique::SANTE, {"sante", "hopital", "medecin", "medical", "soins", "etablissement-sante", "pharmacie"}},
            {Thematique::ENVIRONNEMENT, {"environnement", "ecologie", "climat", "biodiversite", "pollution", "nature", "dechets"}},
            {Thematique::EDUCATION, {"education", "enseignement", "scolaire", "ecole", "college", "lycee", "universite", "formation"}},
            {Thematique::CULTURE, {"culture", "patrimoine", "musee", "bibliotheque", "theatre", "monument", "art"}},
            {Thematique::LOGEMENT, {"logement", "habitat", "immobilier", "hlm", "construction", "urbanisme", "cadastre"}},
            {Thematique::AGRICULTURE, {"agriculture", "agricole", "exploitation", "elevage", "culture", "pac", "bio"}},
            {Thematique::ENERGIE, {"energie", "electricite", "gaz", "renouvelable", "consommation", "production", "eolien", "solaire"}},
            {Thematique::SECURITE, {"securite", "police", "gendarmerie", "delinquance", "accident", "pompier", "prevention"}},
            {Thematique::SOCIAL, {"social", "aide-sociale", "insertion", "solidarite", "handicap", "personnes-agees", "famille"}},
            {Thematique::TOURISME, {"tourisme", "hotel", "camping", "visiteur", "attraction", "sejour", "vacances"}},
            {Thematique::NUMERIQUE, {"numerique", "digital", "internet", "fibre", "couverture", "open-data", "donnees"}},
            {Thematique::TOUTES, {}}
        };
        
        auto it = tagsParThematique.find(theme);
        if (it != tagsParThematique.end()) {
            return it->second;
        }
        return {};
    }

    std::string SearchService::thematiqueVersTag(Thematique theme) {
        switch (theme) {
            case Thematique::ADMINISTRATION: return "administration";
            case Thematique::ECONOMIE: return "economie";
            case Thematique::TRANSPORTS: return "transports";
            case Thematique::SANTE: return "sante";
            case Thematique::ENVIRONNEMENT: return "environnement";
            case Thematique::EDUCATION: return "education";
            case Thematique::CULTURE: return "culture";
            case Thematique::LOGEMENT: return "logement";
            case Thematique::AGRICULTURE: return "agriculture";
            case Thematique::ENERGIE: return "energie";
            case Thematique::SECURITE: return "securite";
            case Thematique::SOCIAL: return "social";
            case Thematique::TOURISME: return "tourisme";
            case Thematique::NUMERIQUE: return "numerique";
            case Thematique::TOUTES: return "";
        }
        return "";
    }

    std::string SearchService::formatVersMimeType(FormatFichier format) {
        switch (format) {
            case FormatFichier::CSV: return "text/csv";
            case FormatFichier::JSON: return "application/json";
            case FormatFichier::GEOJSON: return "application/geo+json";
            case FormatFichier::PARQUET: return "application/parquet";
            case FormatFichier::XML: return "application/xml";
        }
        return "";
    }

    std::optional<FormatFichier> SearchService::mimeTypeVersFormat(const std::string& mimeType) {
        std::string mime = mimeType;
        std::transform(mime.begin(), mime.end(), mime.begin(), ::tolower);
        
        if (mime.find("csv") != std::string::npos || 
            mime.find("comma-separated") != std::string::npos) {
            return FormatFichier::CSV;
        }
        
        if (mime.find("geo+json") != std::string::npos || 
            mime.find("geojson") != std::string::npos) {
            return FormatFichier::GEOJSON;
        }
        
        if (mime.find("json") != std::string::npos) {
            return FormatFichier::JSON;
        }
        
        if (mime.find("parquet") != std::string::npos) {
            return FormatFichier::PARQUET;
        }
        
        if (mime.find("xml") != std::string::npos) {
            return FormatFichier::XML;
        }
        
        return std::nullopt;
    }

    std::vector<std::string> SearchService::getOrganisationsSPD() {
        return {
            "534fff75a3a7292c64a77de4",
            "534fff91a3a7292c64a77e5c",
            "534fff8ea3a7292c64a77e53",
            "534fff94a3a7292c64a77e7e",
            "534fff8ba3a7292c64a77e40",
            "534fff92a3a7292c64a77e6d",
            "5a83f81fc751df6f8573eb8a",
            "534fff81a3a7292c64a77df5",
            "534fff8aa3a7292c64a77e3a",
            "534fff94a3a7292c64a77e79",
            "534fffb5a3a7292c64a78009",
            "5abca8d588ee386ee6ece589",
        };
    }

    std::vector<std::pair<Thematique, std::string>> SearchService::getThematiques() {
        return {
            {Thematique::ADMINISTRATION, "Administration"},
            {Thematique::ECONOMIE, "Économie"},
            {Thematique::TRANSPORTS, "Transports"},
            {Thematique::SANTE, "Santé"},
            {Thematique::ENVIRONNEMENT, "Environnement"},
            {Thematique::EDUCATION, "Éducation"},
            {Thematique::CULTURE, "Culture"},
            {Thematique::LOGEMENT, "Logement"},
            {Thematique::AGRICULTURE, "Agriculture"},
            {Thematique::ENERGIE, "Énergie"},
            {Thematique::SECURITE, "Sécurité"},
            {Thematique::SOCIAL, "Social"},
            {Thematique::TOURISME, "Tourisme"},
            {Thematique::NUMERIQUE, "Numérique"},
        };
    }

    SearchService::SearchService() = default;
    SearchService::~SearchService() = default;

    std::string SearchService::construireURLRecherche(const CriteresRecherche& criteres) const {
        std::ostringstream url;
        url << baseUrl_ << "/datasets/?";
        
        // Normalisation de la requête utilisateur (accents supprimés)
        if (!criteres.requete.empty()) {
            std::string requeteNormalisee = expandreRequete(criteres.requete);
            url << "q=" << urlEncode(requeteNormalisee) << "&";
        }
        
        // Tags explicites de l'utilisateur
        for (const auto& tag : criteres.tags) {
            url << "tag=" << urlEncode(tag) << "&";
        }
        
        if (criteres.organisationId.has_value()) {
            url << "organization=" << urlEncode(*criteres.organisationId) << "&";
        }
        
        if (criteres.codeGeo.has_value()) {
            url << "geozone=" << urlEncode(*criteres.codeGeo) << "&";
        }
        
        if (criteres.schemaRequis.has_value()) {
            url << "schema=" << urlEncode(*criteres.schemaRequis) << "&";
        }
        
        url << "page=" << criteres.page << "&";
        url << "page_size=" << criteres.parPage << "&";
        
        if (!criteres.tri.empty() && criteres.tri != "relevance") {
            std::string sortParam = criteres.tri;
            if (sortParam == "created") sortParam = "-created";
            else if (sortParam == "last_modified") sortParam = "-last_modified";
            else if (sortParam == "downloads") sortParam = "-views";
            url << "sort=" << urlEncode(sortParam) << "&";
        }
        
        std::string result = url.str();
        if (!result.empty() && result.back() == '&') {
            result.pop_back();
        }
        
        return result;
    }

    std::string SearchService::httpGet(const std::string& url) const {
        auto [host, port, path] = parseUrl(url);
        
        if (host.empty()) {
            std::cerr << "[SEARCH] Invalid URL: " << url << std::endl;
            return "";
        }
        
        try {
            net::io_context ioc;
            ssl::context ctx(ssl::context::tlsv12_client);
            
            ctx.set_default_verify_paths();
            ctx.set_verify_mode(ssl::verify_none);
            
            tcp::resolver resolver(ioc);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
            
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                beast::error_code ec{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
                throw beast::system_error{ec};
            }
            
            beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(timeoutSeconds_));
            
            auto const results = resolver.resolve(host, port);
            beast::get_lowest_layer(stream).connect(results);
            stream.handshake(ssl::stream_base::client);
            
            http::request<http::string_body> req{http::verb::get, path, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "CivicCore-HyperIngest/1.0");
            req.set(http::field::accept, "application/json");
            req.set(http::field::connection, "close");
            
            http::write(stream, req);
            
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);
            
            beast::error_code ec;
            stream.shutdown(ec);
            
            if (res.result() != http::status::ok) {
                std::cerr << "[SEARCH] HTTP Error: " << res.result_int() << std::endl;
                return "";
            }
            
            return res.body();
            
        } catch (const std::exception& e) {
            std::cerr << "[SEARCH] HTTP GET Error: " << e.what() << std::endl;
            return "";
        }
    }

    VerificationRessource SearchService::httpHead(const std::string& url) const {
        VerificationRessource result;
        result.disponible = false;
        result.httpStatus = 0;
        
        auto [host, port, path] = parseUrl(url);
        
        if (host.empty()) {
            return result;
        }
        
        auto start = std::chrono::steady_clock::now();
        
        try {
            net::io_context ioc;
            ssl::context ctx(ssl::context::tlsv12_client);
            ctx.set_default_verify_paths();
            ctx.set_verify_mode(ssl::verify_peer);
            
            tcp::resolver resolver(ioc);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);
            
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                return result;
            }
            
            beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
            
            auto const results = resolver.resolve(host, port);
            beast::get_lowest_layer(stream).connect(results);
            stream.handshake(ssl::stream_base::client);
            
            http::request<http::empty_body> req{http::verb::head, path, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "CivicCore-HyperIngest/1.0");
            
            http::write(stream, req);
            
            beast::flat_buffer buffer;
            http::response<http::empty_body> res;
            http::read(stream, buffer, res);
            
            auto end = std::chrono::steady_clock::now();
            result.tempsReponse = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            
            result.httpStatus = res.result_int();
            result.disponible = (res.result() == http::status::ok);
            
            auto ct = res.find(http::field::content_type);
            if (ct != res.end()) {
                result.mimeTypeReel = std::string(ct->value());
            }
            
            auto cl = res.find(http::field::content_length);
            if (cl != res.end()) {
                result.tailleReelle = std::stoll(std::string(cl->value()));
            }
            
            beast::error_code ec;
            stream.shutdown(ec);
            
        } catch (const std::exception& e) {
            auto end = std::chrono::steady_clock::now();
            result.tempsReponse = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        }
        
        return result;
    }

    ResultatRecherche SearchService::parserReponse(const std::string& json,
                                                    const CriteresRecherche& criteres,
                                                    std::chrono::milliseconds tempsRecherche) const {
        ResultatRecherche resultat;
        resultat.tempsRecherche = tempsRecherche;
        resultat.pageCourante = criteres.page;
        
        simdjson::dom::parser parser;
        simdjson::padded_string padded(json);
        
        simdjson::dom::element doc;
        auto error = parser.parse(padded).get(doc);
        if (error) {
            std::cerr << "[SEARCH] JSON Parse Error: " << error << std::endl;
            return resultat;
        }
        
        int64_t total = 0;
        doc["total"].get(total);
        resultat.totalResultats = static_cast<int>(total);
        resultat.totalPages = (resultat.totalResultats + criteres.parPage - 1) / criteres.parPage;
        
        simdjson::dom::array data;
        if (doc["data"].get(data) != simdjson::SUCCESS) {
            return resultat;
        }
        
        for (auto datasetEl : data) {
            JeuDeDonnees jeu;
            
            std::string_view sv;
            if (datasetEl["id"].get(sv) == simdjson::SUCCESS) jeu.id = std::string(sv);
            if (datasetEl["slug"].get(sv) == simdjson::SUCCESS) jeu.slug = std::string(sv);
            if (datasetEl["title"].get(sv) == simdjson::SUCCESS) jeu.titre = std::string(sv);
            if (datasetEl["description"].get(sv) == simdjson::SUCCESS) jeu.description = std::string(sv);
            if (datasetEl["license"].get(sv) == simdjson::SUCCESS) jeu.licence = std::string(sv);
            
            simdjson::dom::element org;
            if (datasetEl["organization"].get(org) == simdjson::SUCCESS) {
                if (org["name"].get(sv) == simdjson::SUCCESS) jeu.organisation = std::string(sv);
                if (org["id"].get(sv) == simdjson::SUCCESS) jeu.organisationId = std::string(sv);
                
                simdjson::dom::array badges;
                if (org["badges"].get(badges) == simdjson::SUCCESS) {
                    for (auto badge : badges) {
                        std::string_view kind;
                        if (badge["kind"].get(kind) == simdjson::SUCCESS) {
                            if (kind == "public-service" || kind == "certified" || kind == "spd") {
                                jeu.organisationCertifiee = true;
                            }
                        }
                    }
                }
            }
            
            if (criteres.uniquementCertifiees && !jeu.organisationCertifiee) {
                continue;
            }
            
            if (datasetEl["created_at"].get(sv) == simdjson::SUCCESS) {
                jeu.dateCreation = parseISODate(std::string(sv));
            }
            if (datasetEl["last_modified"].get(sv) == simdjson::SUCCESS) {
                jeu.derniereMaj = parseISODate(std::string(sv));
            }
            
            simdjson::dom::array tags;
            if (datasetEl["tags"].get(tags) == simdjson::SUCCESS) {
                for (auto tag : tags) {
                    if (tag.get(sv) == simdjson::SUCCESS) {
                        jeu.tags.push_back(std::string(sv));
                    }
                }
            }
            
            simdjson::dom::element spatial;
            if (datasetEl["spatial"].get(spatial) == simdjson::SUCCESS) {
                if (spatial["granularity"].get(sv) == simdjson::SUCCESS) {
                    jeu.granulariteTerritoriale = std::string(sv);
                }
            }
            
            if (criteres.granularite != Territoire::TOUS) {
                std::string granulariteRequise;
                switch (criteres.granularite) {
                    case Territoire::NATIONAL: granulariteRequise = "country"; break;
                    case Territoire::REGIONAL: granulariteRequise = "fr:region"; break;
                    case Territoire::DEPARTEMENTAL: granulariteRequise = "fr:departement"; break;
                    case Territoire::COMMUNAL: granulariteRequise = "fr:commune"; break;
                    case Territoire::EPCI: granulariteRequise = "fr:epci"; break;
                    default: break;
                }
                
                if (!granulariteRequise.empty() && 
                    jeu.granulariteTerritoriale.find(granulariteRequise) == std::string::npos) {
                    continue;
                }
            }
            
            simdjson::dom::element metrics;
            if (datasetEl["metrics"].get(metrics) == simdjson::SUCCESS) {
                int64_t views = 0, reuses = 0;
                metrics["views"].get(views);
                metrics["reuses"].get(reuses);
                jeu.nombreTelechargements = static_cast<int>(views);
                jeu.nombreReutilisations = static_cast<int>(reuses);
            }
            
            simdjson::dom::array resources;
            if (datasetEl["resources"].get(resources) == simdjson::SUCCESS) {
                for (auto resEl : resources) {
                    Ressource res;
                    
                    if (resEl["id"].get(sv) == simdjson::SUCCESS) res.id = std::string(sv);
                    if (resEl["title"].get(sv) == simdjson::SUCCESS) res.titre = std::string(sv);
                    if (resEl["description"].get(sv) == simdjson::SUCCESS) res.description = std::string(sv);
                    if (resEl["url"].get(sv) == simdjson::SUCCESS) res.url = std::string(sv);
                    
                    std::string format, mime;
                    if (resEl["format"].get(sv) == simdjson::SUCCESS) format = std::string(sv);
                    if (resEl["mime"].get(sv) == simdjson::SUCCESS) mime = std::string(sv);
                    res.mimeType = mime.empty() ? format : mime;
                    
                    auto formatOpt = mimeTypeVersFormat(res.mimeType);
                    if (!formatOpt && !format.empty()) {
                        formatOpt = mimeTypeVersFormat(format);
                    }
                    if (formatOpt) {
                        res.format = *formatOpt;
                    }
                    
                    int64_t size = 0;
                    resEl["filesize"].get(size);
                    res.taille = size;
                    
                    if (resEl["last_modified"].get(sv) == simdjson::SUCCESS) {
                        res.derniereMaj = parseISODate(std::string(sv));
                    }
                    
                    if (resEl["type"].get(sv) == simdjson::SUCCESS) {
                        res.estPrincipale = (sv == "main");
                    } else {
                        res.estPrincipale = true;
                    }
                    
                    simdjson::dom::element schema;
                    if (resEl["schema"].get(schema) == simdjson::SUCCESS) {
                        if (schema["name"].get(sv) == simdjson::SUCCESS) {
                            res.schema = std::string(sv);
                        }
                    }
                    
                    int64_t status = 200;
                    resEl["extras"].at_pointer("/check:status").get(status);
                    res.httpStatus = static_cast<int>(status);
                    
                    jeu.ressources.push_back(std::move(res));
                }
            }
            
            jeu.ressources = filtrerRessources(jeu.ressources, criteres);
            
            if (!jeu.ressources.empty()) {
                resultat.jeux.push_back(std::move(jeu));
            }
        }
        
        return resultat;
    }

    bool SearchService::ressourceAcceptee(const Ressource& ressource,
                                           const CriteresRecherche& criteres) const {
        auto formatOpt = mimeTypeVersFormat(ressource.mimeType);
        if (formatOpt) {
            if (criteres.formatsAcceptes.find(*formatOpt) == criteres.formatsAcceptes.end()) {
                return false;
            }
        } else {
            std::string mime = ressource.mimeType;
            std::transform(mime.begin(), mime.end(), mime.begin(), ::tolower);
            
            if (criteres.exclurePDF && mime.find("pdf") != std::string::npos) {
                return false;
            }
            if (criteres.exclureImages && 
                (mime.find("image") != std::string::npos ||
                 mime.find("png") != std::string::npos ||
                 mime.find("jpg") != std::string::npos ||
                 mime.find("jpeg") != std::string::npos ||
                 mime.find("gif") != std::string::npos)) {
                return false;
            }
        }
        
        if (criteres.uniquementRessourcePrincipale && !ressource.estPrincipale) {
            return false;
        }
        
        if (criteres.schemaRequis.has_value()) {
            if (!ressource.schema.has_value() || 
                ressource.schema->find(*criteres.schemaRequis) == std::string::npos) {
                return false;
            }
        }
        
        if (criteres.ageMaxJours.has_value()) {
            auto now = std::chrono::system_clock::now();
            auto age = std::chrono::duration_cast<std::chrono::hours>(now - ressource.derniereMaj).count() / 24;
            if (age > *criteres.ageMaxJours) {
                return false;
            }
        }
        
        if (criteres.miseAJourApres.has_value()) {
            if (ressource.derniereMaj < *criteres.miseAJourApres) {
                return false;
            }
        }
        
        return true;
    }

    std::vector<Ressource> SearchService::filtrerRessources(const std::vector<Ressource>& ressources,
                                                             const CriteresRecherche& criteres) const {
        std::vector<Ressource> resultat;
        
        for (const auto& res : ressources) {
            if (ressourceAcceptee(res, criteres)) {
                Ressource filteredRes = res;
                
                if (criteres.verifierDisponibilite && !res.url.empty()) {
                    auto verif = httpHead(res.url);
                    filteredRes.httpStatus = verif.httpStatus;
                    
                    if (!verif.disponible) {
                        continue;
                    }
                    
                    if (verif.mimeTypeReel.has_value()) {
                        filteredRes.mimeType = *verif.mimeTypeReel;
                    }
                }
                
                resultat.push_back(std::move(filteredRes));
            }
        }
        
        return resultat;
    }

    ResultatRecherche SearchService::rechercher(const CriteresRecherche& criteres) {
        auto start = std::chrono::steady_clock::now();
        
        std::string url = construireURLRecherche(criteres);
        std::cout << "[SEARCH] Query: " << url << std::endl;
        
        std::string json = httpGet(url);
        
        auto end = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        if (json.empty()) {
            ResultatRecherche empty;
            empty.requeteAPI = url;
            empty.tempsRecherche = elapsed;
            return empty;
        }
        
        auto resultat = parserReponse(json, criteres, elapsed);
        resultat.requeteAPI = url;
        
        std::cout << "[SEARCH] Found " << resultat.jeux.size() << " datasets with valid resources ("
                  << resultat.totalResultats << " total)" << std::endl;
        
        return resultat;
    }

    void SearchService::rechercherAsync(const CriteresRecherche& criteres, SearchCallback callback) {
        auto resultat = rechercher(criteres);
        if (callback) {
            callback(std::move(resultat));
        }
    }

    VerificationRessource SearchService::verifierRessource(const std::string& url) {
        auto result = httpHead(url);
        result.resourceId = url;
        return result;
    }

    void SearchService::verifierRessourceAsync(const std::string& url, VerifyCallback callback) {
        auto result = verifierRessource(url);
        if (callback) {
            callback(std::move(result));
        }
    }

    std::optional<JeuDeDonnees> SearchService::getDataset(const std::string& datasetId) {
        std::string url = baseUrl_ + "/datasets/" + datasetId + "/";
        std::string json = httpGet(url);
        
        if (json.empty()) {
            return std::nullopt;
        }
        
        simdjson::dom::parser parser;
        simdjson::padded_string padded(json);
        
        simdjson::dom::element doc;
        if (parser.parse(padded).get(doc) != simdjson::SUCCESS) {
            return std::nullopt;
        }
        
        std::ostringstream wrapper;
        wrapper << R"({"data":[)" << json << R"(],"total":1})";
        
        CriteresRecherche criteres;
        criteres.verifierDisponibilite = false;
        auto result = parserReponse(wrapper.str(), criteres, std::chrono::milliseconds(0));
        
        if (!result.jeux.empty()) {
            return result.jeux[0];
        }
        
        return std::nullopt;
    }

    bool SearchService::telechargerRessource(const Ressource& ressource, 
                                              const std::string& cheminDestination) {
        std::string content = httpGet(ressource.url);
        
        if (content.empty()) {
            return false;
        }
        
        std::ofstream file(cheminDestination, std::ios::binary);
        if (!file) {
            return false;
        }
        
        file.write(content.data(), content.size());
        return file.good();
    }

    ResultatRecherche SearchService::rechercherLocal(const CriteresRecherche& criteres) {
        auto start = std::chrono::steady_clock::now();

        std::ifstream file("/data_enriched.json");
        if (!file.is_open()) {
            std::cerr << "[SEARCH-LOCAL] Erreur: Impossible d'ouvrir data_enriched.json" << std::endl;
            return {};
        }

        std::string json_content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        file.close();

        simdjson::dom::parser parser;
        simdjson::padded_string padded(json_content);
        simdjson::dom::array doc;
        auto error = parser.parse(padded).get(doc);

        if (error) {
            std::cerr << "[SEARCH-LOCAL] Erreur de parsing JSON: " << error << std::endl;
            return {};
        }

        std::vector<JeuDeDonnees> all_matches;

        // Traitement de la requête textuelle
        std::vector<std::string> query_words;
        if (!criteres.requete.empty()) {
            std::string requete_norm = normaliserTexte(criteres.requete);
            std::stringstream ss(requete_norm);
            std::string word;
            while (ss >> word) {
                query_words.push_back(word);
            }
        }
        
        // Tags de la thématique
        std::vector<std::string> tags_thematique = getTagsThematique(criteres.thematique);

        for (simdjson::dom::element datasetEl : doc) {
            bool match = true;

            // 1. Filtrage par certification
            bool is_certified = false;
            simdjson::dom::element org_el;
            if (datasetEl["organization"].get(org_el) == simdjson::SUCCESS) {
                simdjson::dom::array badges;
                if (org_el["badges"].get(badges) == simdjson::SUCCESS) {
                    for (auto badge : badges) {
                        std::string_view kind;
                        if (badge["kind"].get(kind) == simdjson::SUCCESS && (kind == "public-service" || kind == "certified" || kind == "spd")) {
                            is_certified = true;
                            break;
                        }
                    }
                }
            }
            if (criteres.uniquementCertifiees && !is_certified) {
                continue; // Skip non-certified if required
            }

            // 2. Filtrage textuel (le coeur de la recherche)
            if (!query_words.empty()) {
                std::string text_corpus;
                std::string_view sv;

                if (datasetEl["title"].get(sv) == simdjson::SUCCESS) text_corpus += std::string(sv) + " ";
                if (datasetEl["description"].get(sv) == simdjson::SUCCESS) text_corpus += std::string(sv) + " ";

                simdjson::dom::array tags;
                if (datasetEl["tags"].get(tags) == simdjson::SUCCESS) {
                    for (auto tag : tags) {
                        if (tag.get(sv) == simdjson::SUCCESS) text_corpus += std::string(sv) + " ";
                    }
                }
                
                simdjson::dom::array enriched_keywords;
                if (datasetEl["enriched_keywords"].get(enriched_keywords) == simdjson::SUCCESS) {
                    for (auto keyword : enriched_keywords) {
                        if (keyword.get(sv) == simdjson::SUCCESS) text_corpus += std::string(sv) + " ";
                    }
                }

                std::string corpus_norm = normaliserTexte(text_corpus);
                
                for (const auto& word : query_words) {
                    if (corpus_norm.find(word) == std::string::npos) {
                        match = false;
                        break;
                    }
                }
            }
            if (!match) continue;
            
            // 3. Filtrage par thématique
            if (criteres.thematique != Thematique::TOUTES) {
                bool theme_match = false;
                simdjson::dom::array tags;
                if (datasetEl["tags"].get(tags) == simdjson::SUCCESS) {
                    for (auto tag_el : tags) {
                        std::string_view tag_sv;
                        if (tag_el.get(tag_sv) == simdjson::SUCCESS) {
                            for (const auto& theme_tag : tags_thematique) {
                                if (theme_tag == tag_sv) {
                                    theme_match = true;
                                    break;
                                }
                            }
                        }
                        if (theme_match) break;
                    }
                }
                if (!theme_match) continue;
            }

            JeuDeDonnees jeu;
            std::string_view sv;
            if (datasetEl["id"].get(sv) == simdjson::SUCCESS) jeu.id = std::string(sv);
            if (datasetEl["title"].get(sv) == simdjson::SUCCESS) jeu.titre = std::string(sv);
            if (datasetEl["description"].get(sv) == simdjson::SUCCESS) jeu.description = std::string(sv);
            if (datasetEl["organization"]["name"].get(sv) == simdjson::SUCCESS) jeu.organisation = std::string(sv);
            jeu.organisationCertifiee = is_certified;
            
            simdjson::dom::array resources;
            if (datasetEl["resources"].get(resources) == simdjson::SUCCESS) {
                for (auto resEl : resources) {
                    Ressource res;
                    if (resEl["url"].get(sv) == simdjson::SUCCESS) res.url = std::string(sv);
                    if (resEl["title"].get(sv) == simdjson::SUCCESS) res.titre = std::string(sv);
                    std::string mime;
                    if (resEl["mime"].get(sv) == simdjson::SUCCESS) mime = std::string(sv);
                    auto formatOpt = mimeTypeVersFormat(mime);
                    if(formatOpt.has_value()) res.format = formatOpt.value();
                    jeu.ressources.push_back(res);
                }
            }
            all_matches.push_back(std::move(jeu));
        }

        // Pagination
        ResultatRecherche resultat;
        resultat.totalResultats = all_matches.size();
        resultat.pageCourante = criteres.page;
        resultat.totalPages = (resultat.totalResultats > 0 && criteres.parPage > 0) ? (resultat.totalResultats + criteres.parPage - 1) / criteres.parPage : 0;

        int start_index = (criteres.page - 1) * criteres.parPage;
        
        if (start_index < (int)all_matches.size()) {
            int end_index = std::min(start_index + criteres.parPage, (int)all_matches.size());
            for (int i = start_index; i < end_index; ++i) {
                resultat.jeux.push_back(all_matches[i]);
            }
        }
        
        auto end = std::chrono::steady_clock::now();
        resultat.tempsRecherche = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        std::cout << "[SEARCH-LOCAL] Found " << resultat.jeux.size() << " datasets on this page ("
                  << resultat.totalResultats << " total)" << std::endl;

        return resultat;
    }

}
