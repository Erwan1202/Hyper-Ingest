#include "data/StorageEngine.hpp"
#include <iostream>
#include <chrono>

namespace civic {

    StorageEngine::StorageEngine(const std::string& dbPath) 
        : db_(dbPath.empty() ? nullptr : dbPath.c_str()), con_(db_), parser_() 
    {
        con_.Query(R"(
            CREATE TABLE IF NOT EXISTS ingest_logs (
                ingest_ts TIMESTAMP,
                author VARCHAR,
                title VARCHAR,
                raw_data JSON
            );
        )");
        
        std::cout << "[DB] DuckDB Storage Engine initialized at: " << dbPath << std::endl;
    }

    StorageEngine::~StorageEngine() {
        std::cout << "[DB] Shutting down Storage Engine." << std::endl;
    }

    void StorageEngine::ingest(const std::string& rawJson) {
        std::lock_guard<std::mutex> lock(writeMutex_);

        try {
            simdjson::padded_string padded = rawJson;
            simdjson::dom::element doc = parser_.parse(padded);

            std::string author = "Unknown";
            std::string title = "Untitled";

            simdjson::dom::element slideshow;
            if (doc["slideshow"].get(slideshow) == simdjson::SUCCESS) {
                std::string_view sv_author, sv_title;
                if (slideshow["author"].get(sv_author) == simdjson::SUCCESS) {
                    author = std::string(sv_author);
                }
                if (slideshow["title"].get(sv_title) == simdjson::SUCCESS) {
                    title = std::string(sv_title);
                }
            }

            auto stmt = con_.Prepare("INSERT INTO ingest_logs VALUES (now(), $1, $2, $3)");
            if (stmt->HasError()) {
                std::cerr << "[DB] Prepare Error: " << stmt->GetError() << std::endl;
                return;
            }
            auto result = stmt->Execute(author, title, rawJson);

            if (result->HasError()) {
                std::cerr << "[DB] Insert Error: " << result->GetError() << std::endl;
            }

        } catch (const simdjson::simdjson_error& e) {
            std::cerr << "[PARSE] JSON Error: " << e.what() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "[DB] Critical Error: " << e.what() << std::endl;
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