#include "data/StorageEngine.hpp"
#include <iostream>
#include <mutex>

namespace civic {

    static std::mutex g_writeMutex;

    StorageEngine::StorageEngine(const std::string& dbPath) 
        : db_(dbPath == ":memory:" ? nullptr : dbPath.c_str()), parser_() 
    {
        // Créer une connexion temporaire pour initialiser le schéma
        duckdb::Connection con(db_);

        auto result = con.Query(R"(
            CREATE TABLE IF NOT EXISTS ingest_logs (
                ingest_ts TIMESTAMP,
                author VARCHAR,
                title VARCHAR,
                raw_data TEXT
            );
        )");
        
        if (result->HasError()) {
            std::cerr << "[DB] FATAL: Schema Creation Failed: " << result->GetError() << std::endl;
            exit(1);
        }

        auto check = con.Query("SELECT count(*) FROM ingest_logs");
        if (check->HasError()) {
             std::cerr << "[DB] FATAL: Self-Check Failed - Table missing!" << std::endl;
             exit(1);
        }
        
        std::cout << "[DB] Storage Engine Ready (" << dbPath << ") - Self Check OK." << std::endl;
    }

    StorageEngine::~StorageEngine() {}

    std::unique_ptr<duckdb::Connection> StorageEngine::createConnection() {
        return std::make_unique<duckdb::Connection>(db_);
    }

    void StorageEngine::ingest(duckdb::Connection& con, const std::string& rawJson) {
        std::lock_guard<std::mutex> lock(g_writeMutex);
        
        simdjson::padded_string padded = rawJson;
        simdjson::dom::element doc;
        auto err = parser_.parse(padded).get(doc);
        if (err) { return; }

        std::string author = "Unknown", title = "Untitled";
        
        simdjson::dom::element slideshow;
        if (doc["slideshow"].get(slideshow) == simdjson::SUCCESS) {
             std::string_view sv;
             if (slideshow["author"].get(sv) == simdjson::SUCCESS) author = std::string(sv);
             if (slideshow["title"].get(sv) == simdjson::SUCCESS) title = std::string(sv);
        }

        auto stmt = con.Prepare("INSERT INTO ingest_logs VALUES (now(), ?, ?, ?)");
        if(!stmt->success) {
            std::cerr << "[DB] Prepare Fail: " << stmt->error.Message() << std::endl;
            return;
        }
        
        stmt->Execute(author, title, rawJson);
    }

    void StorageEngine::query(duckdb::Connection& con, const std::string& sql) {
        std::lock_guard<std::mutex> lock(g_writeMutex);
        auto result = con.Query(sql);
        if (!result->HasError()) {
            result->Print();
        } else {
            std::cerr << "[DB] Query Error: " << result->GetError() << std::endl;
        }
    }
}