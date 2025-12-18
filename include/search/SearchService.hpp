#pragma once

#include <string>
#include <vector>
#include <optional>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_set>

namespace civic {

    enum class Thematique {
        ADMINISTRATION,
        ECONOMIE,
        TRANSPORTS,
        SANTE,
        ENVIRONNEMENT,
        EDUCATION,
        CULTURE,
        LOGEMENT,
        AGRICULTURE,
        ENERGIE,
        SECURITE,
        SOCIAL,
        TOURISME,
        NUMERIQUE,
        TOUTES
    };

    enum class SourceType {
        INSEE,
        MINISTERE,
        COLLECTIVITE_SPD,
        OPERATEUR_NATIONAL,
        ETABLISSEMENT_PUBLIC,
        TOUTES
    };

    enum class Territoire {
        NATIONAL,
        REGIONAL,
        DEPARTEMENTAL,
        COMMUNAL,
        EPCI,
        TOUS
    };

    enum class FormatFichier {
        CSV,
        JSON,
        GEOJSON,
        PARQUET,
        XML
    };

    struct Ressource {
        std::string id;
        std::string titre;
        std::string description;
        std::string url;
        FormatFichier format;
        std::string mimeType;
        int64_t taille;
        std::chrono::system_clock::time_point derniereMaj;
        bool estPrincipale;
        std::optional<std::string> schema;
        int httpStatus;
        
        bool estValide() const { return httpStatus == 200; }
        bool estConforme() const { return schema.has_value(); }
    };

    struct JeuDeDonnees {
        std::string id;
        std::string slug;
        std::string titre;
        std::string description;
        std::string organisation;
        std::string organisationId;
        bool organisationCertifiee;
        Thematique thematique;
        std::vector<std::string> tags;
        std::string couvertureTerritoriale;
        std::string granulariteTerritoriale;
        std::chrono::system_clock::time_point dateCreation;
        std::chrono::system_clock::time_point derniereMaj;
        int frequenceMaj;
        std::vector<Ressource> ressources;
        std::string licence;
        int nombreTelechargements;
        int nombreReutilisations;
        double score;
    };

    struct CriteresRecherche {
        Thematique thematique = Thematique::TOUTES;
        std::string requete;
        std::vector<std::string> tags;
        SourceType source = SourceType::TOUTES;
        std::optional<std::string> organisationId;
        bool uniquementCertifiees = false;
        Territoire granularite = Territoire::TOUS;
        std::optional<std::string> codeGeo;
        std::unordered_set<FormatFichier> formatsAcceptes = {
            FormatFichier::CSV, 
            FormatFichier::JSON, 
            FormatFichier::GEOJSON
        };
        bool exclurePDF = true;
        bool exclureImages = true;
        bool uniquementRessourcePrincipale = true;
        bool verifierDisponibilite = true;
        std::optional<std::string> schemaRequis;
        std::optional<std::chrono::system_clock::time_point> miseAJourApres;
        std::optional<int> ageMaxJours;
        int page = 1;
        int parPage = 20;
        std::string tri = "relevance";
    };

    struct ResultatRecherche {
        std::vector<JeuDeDonnees> jeux;
        int totalResultats;
        int pageCourante;
        int totalPages;
        std::chrono::milliseconds tempsRecherche;
        std::string requeteAPI;
    };

    struct VerificationRessource {
        std::string resourceId;
        bool disponible;
        int httpStatus;
        std::optional<std::string> mimeTypeReel;
        std::optional<int64_t> tailleReelle;
        std::chrono::milliseconds tempsReponse;
    };

    class SearchService {
    public:
        using SearchCallback = std::function<void(ResultatRecherche)>;
        using VerifyCallback = std::function<void(VerificationRessource)>;

        SearchService();
        ~SearchService();

        ResultatRecherche rechercher(const CriteresRecherche& criteres);
        ResultatRecherche rechercherLocal(const CriteresRecherche& criteres);
        void rechercherAsync(const CriteresRecherche& criteres, SearchCallback callback);
        VerificationRessource verifierRessource(const std::string& url);
        void verifierRessourceAsync(const std::string& url, VerifyCallback callback);
        std::optional<JeuDeDonnees> getDataset(const std::string& datasetId);
        bool telechargerRessource(const Ressource& ressource, const std::string& cheminDestination);

        static std::string thematiqueVersTag(Thematique theme);
        static std::vector<std::string> getTagsThematique(Thematique theme);
        static std::string formatVersMimeType(FormatFichier format);
        static std::optional<FormatFichier> mimeTypeVersFormat(const std::string& mimeType);
        static std::vector<std::string> getOrganisationsSPD();
        static std::vector<std::pair<Thematique, std::string>> getThematiques();

    private:
        std::string construireURLRecherche(const CriteresRecherche& criteres) const;
        ResultatRecherche parserReponse(const std::string& json, 
                                         const CriteresRecherche& criteres,
                                         std::chrono::milliseconds tempsRecherche) const;
        std::vector<Ressource> filtrerRessources(const std::vector<Ressource>& ressources,
                                                  const CriteresRecherche& criteres) const;
        bool ressourceAcceptee(const Ressource& ressource, 
                               const CriteresRecherche& criteres) const;
        std::string httpGet(const std::string& url) const;
        VerificationRessource httpHead(const std::string& url) const;

        std::string baseUrl_ = "https://www.data.gouv.fr/api/1";
        int timeoutSeconds_ = 30;
    };

    class CriteresBuilder {
    public:
        CriteresBuilder& thematique(Thematique t) { criteres_.thematique = t; return *this; }
        CriteresBuilder& requete(const std::string& q) { criteres_.requete = q; return *this; }
        CriteresBuilder& tag(const std::string& t) { criteres_.tags.push_back(t); return *this; }
        CriteresBuilder& source(SourceType s) { criteres_.source = s; return *this; }
        CriteresBuilder& organisation(const std::string& orgId) { criteres_.organisationId = orgId; return *this; }
        CriteresBuilder& certifieesUniquement(bool b = true) { criteres_.uniquementCertifiees = b; return *this; }
        CriteresBuilder& territoire(Territoire t) { criteres_.granularite = t; return *this; }
        CriteresBuilder& codeGeo(const std::string& code) { criteres_.codeGeo = code; return *this; }
        CriteresBuilder& format(FormatFichier f) { criteres_.formatsAcceptes.insert(f); return *this; }
        CriteresBuilder& formatsStricts(std::initializer_list<FormatFichier> formats) { 
            criteres_.formatsAcceptes.clear();
            for (auto f : formats) criteres_.formatsAcceptes.insert(f);
            return *this; 
        }
        CriteresBuilder& schema(const std::string& s) { criteres_.schemaRequis = s; return *this; }
        CriteresBuilder& miseAJourDepuis(int jours) { criteres_.ageMaxJours = jours; return *this; }
        CriteresBuilder& ressourcePrincipaleUniquement(bool b = true) { criteres_.uniquementRessourcePrincipale = b; return *this; }
        CriteresBuilder& verifierDisponibilite(bool b = true) { criteres_.verifierDisponibilite = b; return *this; }
        CriteresBuilder& page(int p) { criteres_.page = p; return *this; }
        CriteresBuilder& parPage(int pp) { criteres_.parPage = pp; return *this; }
        CriteresBuilder& tri(const std::string& t) { criteres_.tri = t; return *this; }
        CriteresRecherche build() const { return criteres_; }
        
    private:
        CriteresRecherche criteres_;
    };

}
