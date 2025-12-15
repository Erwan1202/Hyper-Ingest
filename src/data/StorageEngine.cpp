#include "data/StorageEngine.hpp"
#include <iostream>

namespace civic {

    StorageEngine::StorageEngine(const std::string& dbPath) 
        : db_(dbPath == ":memory:" ? nullptr : dbPath.c_str()), con_(db_), parser_() 
    {


        auto result = con_.Query(R"(
            CREATE TABLE ingest_logs (
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

        auto check = con_.Query("SELECT count(*) FROM ingest_logs");
        if (check->HasError()) {
             std::cerr << "[DB] FATAL: Self-Check Failed - Table missing!" << std::endl;
             exit(1);
        }
        
        std::cout << "[DB] Storage Engine Ready (" << dbPath << ") - Self Check OK." << std::endl;
    }

    StorageEngine::~StorageEngine() {}

    void StorageEngine::ingest(const std::string& rawJson) {
        std::lock_guard<std::mutex> lock(writeMutex_);
        
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

        auto stmt = con_.Prepare("INSERT INTO ingest_logs VALUES (now(), ?, ?, ?)");
        if(!stmt->success) {
            std::cerr << "[DB] Prepare Fail: " << stmt->error.Message() << std::endl;
            return;
        }
        
        stmt->Execute(author, title, rawJson);
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