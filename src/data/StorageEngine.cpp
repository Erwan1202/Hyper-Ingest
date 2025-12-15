#include "data/StorageEngine.hpp"
#include <iostream>
#include <functional>

namespace civic {

    StorageEngine::StorageEngine(const std::string& dbPath, size_t maxRecords) 
        : db_(dbPath == ":memory:" ? nullptr : dbPath.c_str()), con_(db_), parser_(),
          maxRecords_(maxRecords)
    {
        auto result = con_.Query(R"(
            CREATE TABLE IF NOT EXISTS ingest_logs (
                id INTEGER PRIMARY KEY,
                ingest_ts TIMESTAMP,
                author VARCHAR,
                title VARCHAR,
                raw_data TEXT
            );
            CREATE SEQUENCE IF NOT EXISTS ingest_seq;
        )");
        
        if (result->HasError()) {
            std::cerr << "[DB] FATAL: Schema Creation Failed: " << result->GetError() << std::endl;
            exit(1);
        }

        auto check = con_.Query("SELECT count(*) FROM ingest_logs");
        if (check->HasError()) {
             std::cerr << "[DB] FATAL: Self-Check Failed - Table missing!" << std::endl;
             exit(1);
        }
        
        std::cout << "[DB] Storage Engine Ready (" << dbPath << ") - Max " << maxRecords_ << " records" << std::endl;
    }

    StorageEngine::~StorageEngine() {
        flush(); // Write remaining batch on shutdown
    }

    size_t StorageEngine::hashContent(const std::string& content) {
        return std::hash<std::string>{}(content);
    }

    bool StorageEngine::ingest(const std::string& rawJson) {
        std::lock_guard<std::mutex> lock(writeMutex_);
        
        // Parser le JSON d'abord pour avoir author/title
        simdjson::padded_string padded = rawJson;
        simdjson::dom::element doc;
        auto err = parser_.parse(padded).get(doc);
        if (err) { return false; }

        std::string author = "Unknown", title = "Untitled";
        
        simdjson::dom::element slideshow;
        if (doc["slideshow"].get(slideshow) == simdjson::SUCCESS) {
             std::string_view sv;
             if (slideshow["author"].get(sv) == simdjson::SUCCESS) author = std::string(sv);
             if (slideshow["title"].get(sv) == simdjson::SUCCESS) title = std::string(sv);
        }
        
        // Déduplication : ignorer si déjà vu
        size_t hash = hashContent(rawJson);
        if (seenHashes_.count(hash)) {
            if (displayCallback_) displayCallback_(author, title, true); // true = duplicate
            return false; // Doublon ignoré
        }
        seenHashes_.insert(hash);
        
        // Callback pour affichage
        if (displayCallback_) displayCallback_(author, title, false); // false = new

        // Ajouter au batch
        batch_.emplace_back(author, title, rawJson);
        
        // Écrire si batch plein
        if (batch_.size() >= BATCH_SIZE) {
            writeBatch();
        }
        
        return true;
    }

    void StorageEngine::flush() {
        std::lock_guard<std::mutex> lock(writeMutex_);
        if (!batch_.empty()) {
            writeBatch();
        }
    }

    void StorageEngine::writeBatch() {
        if (batch_.empty()) return;
        
        // Transaction pour performance
        con_.Query("BEGIN TRANSACTION");
        
        auto stmt = con_.Prepare("INSERT INTO ingest_logs VALUES (nextval('ingest_seq'), now(), ?, ?, ?)");
        if(!stmt->success) {
            std::cerr << "[DB] Prepare Fail: " << stmt->error.Message() << std::endl;
            con_.Query("ROLLBACK");
            return;
        }
        
        for (const auto& [author, title, raw] : batch_) {
            stmt->Execute(author, title, raw);
        }
        
        con_.Query("COMMIT");
        
        std::cout << "[DB] Batch written: " << batch_.size() << " records" << std::endl;
        batch_.clear();
        
        // Appliquer la limite (FIFO)
        enforceLimit();
    }

    void StorageEngine::enforceLimit() {
        auto countResult = con_.Query("SELECT COUNT(*) FROM ingest_logs");
        if (countResult->HasError()) return;
        
        auto count = countResult->GetValue(0, 0).GetValue<int64_t>();
        
        if (count > static_cast<int64_t>(maxRecords_)) {
            size_t toDelete = count - maxRecords_;
            con_.Query("DELETE FROM ingest_logs WHERE id IN (SELECT id FROM ingest_logs ORDER BY id LIMIT " + std::to_string(toDelete) + ")");
            std::cout << "[DB] Pruned " << toDelete << " old records (limit: " << maxRecords_ << ")" << std::endl;
        }
    }

    void StorageEngine::query(const std::string& sql) {
        std::lock_guard<std::mutex> lock(writeMutex_);
        auto result = con_.Query(sql);
        if (!result->HasError()) {
            result->Print();
        } else {
            std::cerr << "[DB] Query Error: " << result->GetError() << std::endl;
        }
    }
}