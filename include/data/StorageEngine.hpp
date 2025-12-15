#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <unordered_set>
#include <functional>
#include <duckdb.hpp>
#include <simdjson.h>

namespace civic {

    class StorageEngine {
    public:
        explicit StorageEngine(const std::string& dbPath = ":memory:", size_t maxRecords = 100000);
        ~StorageEngine();

        // Returns true if stored, false if duplicate
        bool ingest(const std::string& rawJson);
        void flush(); // Force write batch to DB

        void query(const std::string& sql);
        
        // Callback pour affichage
        using DisplayCallback = std::function<void(const std::string&, const std::string&, bool)>;
        void setDisplayCallback(DisplayCallback cb) { displayCallback_ = cb; }

    private:
        void writeBatch();
        void enforceLimit();
        size_t hashContent(const std::string& content);

        duckdb::DuckDB db_;
        duckdb::Connection con_;
        
        simdjson::dom::parser parser_;
        std::mutex writeMutex_;
        
        // Batch & dedup
        std::vector<std::tuple<std::string, std::string, std::string>> batch_;
        std::unordered_set<size_t> seenHashes_;
        size_t maxRecords_;
        static constexpr size_t BATCH_SIZE = 1000;
        
        DisplayCallback displayCallback_;
    };
}