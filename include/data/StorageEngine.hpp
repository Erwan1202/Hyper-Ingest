#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <duckdb.hpp>
#include <simdjson.h>

namespace civic {

    class StorageEngine {
    public:
        explicit StorageEngine(const std::string& dbPath = ":memory:");
        ~StorageEngine();

        void ingest(const std::string& rawJson);

        void query(const std::string& sql);

    private:
        duckdb::DuckDB db_;
        duckdb::Connection con_;
        
        simdjson::dom::parser parser_;
        std::mutex writeMutex_;
    };
}