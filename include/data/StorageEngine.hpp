#pragma once

#include <string>
#include <vector>
#include <memory>
#include <duckdb.hpp>
#include <simdjson.h>

namespace civic {

    class StorageEngine {
    public:
        explicit StorageEngine(const std::string& dbPath = ":memory:");
        ~StorageEngine();

        std::unique_ptr<duckdb::Connection> createConnection();

        void ingest(duckdb::Connection& con, const std::string& rawJson);

        void query(duckdb::Connection& con, const std::string& sql);

    private:
        duckdb::DuckDB db_;
        simdjson::dom::parser parser_;
    };
}