#include <gtest/gtest.h>
#include <chrono>
#include "search/SearchService.hpp"

namespace civic {
namespace test {

// ============================================================================
// TESTS DES CONVERSIONS STATIQUES
// ============================================================================

TEST(SearchServiceTest, ThematiqueVersTagConvertsCorrectly) {
    EXPECT_EQ(SearchService::thematiqueVersTag(Thematique::SANTE), "sante");
    EXPECT_EQ(SearchService::thematiqueVersTag(Thematique::TRANSPORTS), "transports");
    EXPECT_EQ(SearchService::thematiqueVersTag(Thematique::ECONOMIE), "economie");
    EXPECT_EQ(SearchService::thematiqueVersTag(Thematique::TOUTES), "");
}

TEST(SearchServiceTest, FormatVersMimeTypeConvertsCorrectly) {
    EXPECT_EQ(SearchService::formatVersMimeType(FormatFichier::CSV), "text/csv");
    EXPECT_EQ(SearchService::formatVersMimeType(FormatFichier::JSON), "application/json");
    EXPECT_EQ(SearchService::formatVersMimeType(FormatFichier::GEOJSON), "application/geo+json");
    EXPECT_EQ(SearchService::formatVersMimeType(FormatFichier::PARQUET), "application/parquet");
}

TEST(SearchServiceTest, MimeTypeVersFormatConvertsCSV) {
    auto format = SearchService::mimeTypeVersFormat("text/csv");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::CSV);
    
    format = SearchService::mimeTypeVersFormat("application/csv");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::CSV);
}

TEST(SearchServiceTest, MimeTypeVersFormatConvertsJSON) {
    auto format = SearchService::mimeTypeVersFormat("application/json");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::JSON);
}

TEST(SearchServiceTest, MimeTypeVersFormatConvertsGeoJSON) {
    auto format = SearchService::mimeTypeVersFormat("application/geo+json");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::GEOJSON);
    
    // Variantes
    format = SearchService::mimeTypeVersFormat("application/geojson");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::GEOJSON);
}

TEST(SearchServiceTest, MimeTypeVersFormatReturnsNulloptForUnknown) {
    auto format = SearchService::mimeTypeVersFormat("application/pdf");
    EXPECT_FALSE(format.has_value());
    
    format = SearchService::mimeTypeVersFormat("image/png");
    EXPECT_FALSE(format.has_value());
}

TEST(SearchServiceTest, MimeTypeVersFormatIsCaseInsensitive) {
    auto format = SearchService::mimeTypeVersFormat("TEXT/CSV");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::CSV);
    
    format = SearchService::mimeTypeVersFormat("Application/JSON");
    ASSERT_TRUE(format.has_value());
    EXPECT_EQ(*format, FormatFichier::JSON);
}

// ============================================================================
// TESTS DES DONNÉES DE RÉFÉRENCE
// ============================================================================

TEST(SearchServiceTest, GetOrganisationsSPDReturnsNonEmpty) {
    auto orgs = SearchService::getOrganisationsSPD();
    EXPECT_FALSE(orgs.empty());
    EXPECT_GE(orgs.size(), 10u); // Au moins 10 organisations SPD
}

TEST(SearchServiceTest, GetThematiquesReturnsAllThemes) {
    auto themes = SearchService::getThematiques();
    EXPECT_FALSE(themes.empty());
    EXPECT_GE(themes.size(), 10u); // Au moins 10 thématiques
}

// ============================================================================
// TESTS DU CRITERES BUILDER
// ============================================================================

TEST(CriteresBuilderTest, BuildsDefaultCriteria) {
    auto criteres = CriteresBuilder().build();
    
    EXPECT_EQ(criteres.thematique, Thematique::TOUTES);
    EXPECT_TRUE(criteres.requete.empty());
    EXPECT_EQ(criteres.source, SourceType::TOUTES);
    EXPECT_EQ(criteres.granularite, Territoire::TOUS);
    EXPECT_TRUE(criteres.exclurePDF);
    EXPECT_EQ(criteres.page, 1);
    EXPECT_EQ(criteres.parPage, 20);
}

TEST(CriteresBuilderTest, BuildsWithThematique) {
    auto criteres = CriteresBuilder()
        .thematique(Thematique::SANTE)
        .build();
    
    EXPECT_EQ(criteres.thematique, Thematique::SANTE);
}

TEST(CriteresBuilderTest, BuildsWithRequete) {
    auto criteres = CriteresBuilder()
        .requete("pharmacies")
        .build();
    
    EXPECT_EQ(criteres.requete, "pharmacies");
}

TEST(CriteresBuilderTest, BuildsWithMultipleTags) {
    auto criteres = CriteresBuilder()
        .tag("pharmacie")
        .tag("officine")
        .build();
    
    EXPECT_EQ(criteres.tags.size(), 2u);
    EXPECT_EQ(criteres.tags[0], "pharmacie");
    EXPECT_EQ(criteres.tags[1], "officine");
}

TEST(CriteresBuilderTest, BuildsWithTerritoire) {
    auto criteres = CriteresBuilder()
        .territoire(Territoire::REGIONAL)
        .codeGeo("11") // Île-de-France
        .build();
    
    EXPECT_EQ(criteres.granularite, Territoire::REGIONAL);
    ASSERT_TRUE(criteres.codeGeo.has_value());
    EXPECT_EQ(*criteres.codeGeo, "11");
}

TEST(CriteresBuilderTest, BuildsWithStrictFormats) {
    auto criteres = CriteresBuilder()
        .formatsStricts({FormatFichier::CSV, FormatFichier::JSON})
        .build();
    
    EXPECT_EQ(criteres.formatsAcceptes.size(), 2u);
    EXPECT_TRUE(criteres.formatsAcceptes.count(FormatFichier::CSV));
    EXPECT_TRUE(criteres.formatsAcceptes.count(FormatFichier::JSON));
    EXPECT_FALSE(criteres.formatsAcceptes.count(FormatFichier::GEOJSON));
}

TEST(CriteresBuilderTest, BuildsWithCertifiedOnly) {
    auto criteres = CriteresBuilder()
        .certifieesUniquement()
        .build();
    
    EXPECT_TRUE(criteres.uniquementCertifiees);
}

TEST(CriteresBuilderTest, BuildsWithFreshness) {
    auto criteres = CriteresBuilder()
        .miseAJourDepuis(365) // Dernière année
        .build();
    
    ASSERT_TRUE(criteres.ageMaxJours.has_value());
    EXPECT_EQ(*criteres.ageMaxJours, 365);
}

TEST(CriteresBuilderTest, BuildsWithSchema) {
    auto criteres = CriteresBuilder()
        .schema("etalab/schema-irve")
        .build();
    
    ASSERT_TRUE(criteres.schemaRequis.has_value());
    EXPECT_EQ(*criteres.schemaRequis, "etalab/schema-irve");
}

TEST(CriteresBuilderTest, ChainsMultipleOptions) {
    auto criteres = CriteresBuilder()
        .thematique(Thematique::SANTE)
        .requete("pharmacies")
        .tag("officine")
        .certifieesUniquement()
        .territoire(Territoire::REGIONAL)
        .formatsStricts({FormatFichier::CSV})
        .miseAJourDepuis(365)
        .page(2)
        .parPage(50)
        .build();
    
    EXPECT_EQ(criteres.thematique, Thematique::SANTE);
    EXPECT_EQ(criteres.requete, "pharmacies");
    EXPECT_EQ(criteres.tags.size(), 1u);
    EXPECT_TRUE(criteres.uniquementCertifiees);
    EXPECT_EQ(criteres.granularite, Territoire::REGIONAL);
    EXPECT_EQ(criteres.formatsAcceptes.size(), 1u);
    EXPECT_EQ(*criteres.ageMaxJours, 365);
    EXPECT_EQ(criteres.page, 2);
    EXPECT_EQ(criteres.parPage, 50);
}

// ============================================================================
// TESTS DES STRUCTURES DE DONNÉES
// ============================================================================

TEST(RessourceTest, EstValideReturnsTrueFor200) {
    Ressource res;
    res.httpStatus = 200;
    EXPECT_TRUE(res.estValide());
}

TEST(RessourceTest, EstValideReturnsFalseForNon200) {
    Ressource res;
    res.httpStatus = 404;
    EXPECT_FALSE(res.estValide());
    
    res.httpStatus = 500;
    EXPECT_FALSE(res.estValide());
}

TEST(RessourceTest, EstConformeReturnsTrueIfSchemaPresent) {
    Ressource res;
    res.schema = "etalab/schema-test";
    EXPECT_TRUE(res.estConforme());
}

TEST(RessourceTest, EstConformeReturnsFalseIfNoSchema) {
    Ressource res;
    EXPECT_FALSE(res.estConforme());
}

// ============================================================================
// TESTS DU SERVICE DE RECHERCHE
// ============================================================================

TEST(SearchServiceTest, ConstructorCreatesValidInstance) {
    EXPECT_NO_THROW({
        SearchService service;
    });
}

// Note: Les tests suivants nécessitent une connexion réseau
// Ils peuvent être marqués comme DISABLED_ pour les tests CI

TEST(SearchServiceTest, DISABLED_RechercherReturnsResults) {
    SearchService service;
    
    auto criteres = CriteresBuilder()
        .requete("pharmacies")
        .thematique(Thematique::SANTE)
        .formatsStricts({FormatFichier::CSV, FormatFichier::JSON})
        .verifierDisponibilite(false) // Désactiver pour accélérer le test
        .parPage(5)
        .build();
    
    auto resultat = service.rechercher(criteres);
    
    // On s'attend à trouver des résultats
    EXPECT_GT(resultat.totalResultats, 0);
    EXPECT_FALSE(resultat.requeteAPI.empty());
}

TEST(SearchServiceTest, DISABLED_RechercherWithCertifiedOrgsOnly) {
    SearchService service;
    
    auto criteres = CriteresBuilder()
        .requete("entreprises")
        .certifieesUniquement()
        .formatsStricts({FormatFichier::CSV})
        .verifierDisponibilite(false)
        .parPage(5)
        .build();
    
    auto resultat = service.rechercher(criteres);
    
    // Vérifier que tous les résultats viennent d'orgs certifiées
    for (const auto& jeu : resultat.jeux) {
        EXPECT_TRUE(jeu.organisationCertifiee);
    }
}

TEST(SearchServiceTest, DISABLED_VerifierRessourceReturnsStatus) {
    SearchService service;
    
    // URL connue et stable
    auto verif = service.verifierRessource("https://www.data.gouv.fr/api/1/datasets/");
    
    EXPECT_EQ(verif.httpStatus, 200);
    EXPECT_TRUE(verif.disponible);
}

// ============================================================================
// TEST D'INTÉGRATION - Exemple Pharmacies Île-de-France
// ============================================================================

TEST(SearchServiceTest, DISABLED_ExempleParcours_PharmaciesIDF) {
    SearchService service;
    
    // Niveau 1: Catégorie Santé
    // Niveau 2: "Pharmacies" ou "Officines"
    // Niveau 3: Sources certifiées (optionnel)
    // Niveau 4: Île-de-France (code région: 11)
    // Filtres: CSV/JSON, valide, mis à jour cette année
    
    auto criteres = CriteresBuilder()
        .thematique(Thematique::SANTE)
        .requete("pharmacies")
        .tag("officine")
        .territoire(Territoire::REGIONAL)
        .codeGeo("11") // Île-de-France
        .formatsStricts({FormatFichier::CSV, FormatFichier::JSON})
        .miseAJourDepuis(365)
        .ressourcePrincipaleUniquement()
        .verifierDisponibilite(true)
        .parPage(10)
        .build();
    
    auto resultat = service.rechercher(criteres);
    
    std::cout << "\n=== Résultat recherche Pharmacies IDF ===" << std::endl;
    std::cout << "Total: " << resultat.totalResultats << " datasets" << std::endl;
    std::cout << "Temps: " << resultat.tempsRecherche.count() << "ms" << std::endl;
    
    for (const auto& jeu : resultat.jeux) {
        std::cout << "\n- " << jeu.titre << std::endl;
        std::cout << "  Organisation: " << jeu.organisation 
                  << (jeu.organisationCertifiee ? " [SPD]" : "") << std::endl;
        
        for (const auto& res : jeu.ressources) {
            std::cout << "  → " << res.titre << " (" << res.mimeType << ")" << std::endl;
            std::cout << "    URL: " << res.url << std::endl;
            std::cout << "    Status: " << res.httpStatus << std::endl;
        }
    }
    
    // On s'attend à trouver au moins un résultat pertinent
    EXPECT_GT(resultat.jeux.size(), 0u);
}

} // namespace test
} // namespace civic
