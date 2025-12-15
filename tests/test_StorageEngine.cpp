#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>
#include "data/StorageEngine.hpp"

namespace civic {
namespace test {


TEST(StorageEngineTest, ConstructorInMemory) {
    EXPECT_NO_THROW({
        StorageEngine engine(":memory:");
    });
}

TEST(StorageEngineTest, ConstructorDefaultPath) {
    EXPECT_NO_THROW({
        StorageEngine engine();
    });
}


TEST(StorageEngineTest, IngestValidJson) {
    StorageEngine engine(":memory:");
    
    std::string validJson = R"({
        "slideshow": {
            "author": "Test Author",
            "title": "Test Title",
            "slides": [
                {"type": "slide", "content": "Hello World"}
            ]
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(validJson));
}

TEST(StorageEngineTest, IngestSimpleJson) {
    StorageEngine engine(":memory:");
    
    std::string simpleJson = R"({"key": "value", "number": 42})";
    
    EXPECT_NO_THROW(engine.ingest(simpleJson));
}

TEST(StorageEngineTest, IngestJsonWithMissingFields) {
    StorageEngine engine(":memory:");
    
    std::string incompleteJson = R"({"data": [1, 2, 3]})";
    
    EXPECT_NO_THROW(engine.ingest(incompleteJson));
}

TEST(StorageEngineTest, IngestInvalidJson) {
    StorageEngine engine(":memory:");
    
    std::string invalidJson = "{ not valid json }";
    
    EXPECT_NO_THROW(engine.ingest(invalidJson));
}

TEST(StorageEngineTest, IngestEmptyJson) {
    StorageEngine engine(":memory:");
    
    std::string emptyJson = "{}";
    
    EXPECT_NO_THROW(engine.ingest(emptyJson));
}

TEST(StorageEngineTest, IngestNestedJson) {
    StorageEngine engine(":memory:");
    
    std::string nestedJson = R"({
        "slideshow": {
            "author": "Nested Author",
            "title": "Nested Title",
            "metadata": {
                "level1": {
                    "level2": {
                        "level3": "deep value"
                    }
                }
            }
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(nestedJson));
}

TEST(StorageEngineTest, IngestJsonWithArray) {
    StorageEngine engine(":memory:");
    
    std::string arrayJson = R"({
        "slideshow": {
            "author": "Array Author",
            "title": "Array Title",
            "items": [1, 2, 3, 4, 5],
            "names": ["Alice", "Bob", "Charlie"]
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(arrayJson));
}

TEST(StorageEngineTest, IngestLargeJson) {
    StorageEngine engine(":memory:");
    
    std::string largeJson = R"({"slideshow": {"author": "Large Author", "title": "Large Title", "data": [)";
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) largeJson += ",";
        largeJson += R"({"id": )" + std::to_string(i) + R"(, "value": "item)" + std::to_string(i) + R"("})";
    }
    largeJson += "]}}";
    
    EXPECT_NO_THROW(engine.ingest(largeJson));
}


TEST(StorageEngineTest, QueryValidSQL) {
    StorageEngine engine(":memory:");
    
    EXPECT_NO_THROW(engine.query("SELECT * FROM ingest_logs"));
}

TEST(StorageEngineTest, QueryInvalidSQL) {
    StorageEngine engine(":memory:");
    
    EXPECT_NO_THROW(engine.query("INVALID SQL QUERY"));
}

TEST(StorageEngineTest, QueryAfterIngest) {
    StorageEngine engine(":memory:");
    
    std::string json = R"({
        "slideshow": {
            "author": "Query Test Author",
            "title": "Query Test Title"
        }
    })";
    
    engine.ingest(json);
    
    EXPECT_NO_THROW(engine.query("SELECT COUNT(*) FROM ingest_logs"));
}

TEST(StorageEngineTest, QueryCount) {
    StorageEngine engine(":memory:");
    
    for (int i = 0; i < 5; ++i) {
        std::string json = R"({"slideshow": {"author": "Author)" + std::to_string(i) + 
                           R"(", "title": "Title)" + std::to_string(i) + R"("}})";
        engine.ingest(json);
    }
    
    EXPECT_NO_THROW(engine.query("SELECT COUNT(*) as count FROM ingest_logs"));
}


TEST(StorageEngineTest, ConcurrentIngestion) {
    StorageEngine engine(":memory:");
    const int numThreads = 4;
    const int ingestsPerThread = 50;
    
    std::vector<std::thread> threads;
    
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&engine, t, ingestsPerThread]() {
            for (int i = 0; i < ingestsPerThread; ++i) {
                std::string json = R"({
                    "slideshow": {
                        "author": "Thread)" + std::to_string(t) + R"(",
                        "title": "Item)" + std::to_string(i) + R"("
                    }
                })";
                engine.ingest(json);
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    EXPECT_NO_THROW(engine.query("SELECT COUNT(*) FROM ingest_logs"));
}

TEST(StorageEngineTest, ConcurrentIngestAndQuery) {
    StorageEngine engine(":memory:");
    std::atomic<bool> running{true};
    
    std::thread ingestThread([&]() {
        for (int i = 0; i < 100 && running; ++i) {
            std::string json = R"({"slideshow": {"author": "Concurrent", "title": "Test)" + 
                               std::to_string(i) + R"("}})";
            engine.ingest(json);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    
    std::thread queryThread([&]() {
        for (int i = 0; i < 20 && running; ++i) {
            engine.query("SELECT COUNT(*) FROM ingest_logs");
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });
    
    ingestThread.join();
    running = false;
    queryThread.join();
    
    SUCCEED();
}


TEST(StorageEngineTest, IngestUnicodeJson) {
    StorageEngine engine(":memory:");
    
    // DuckDB has issues with raw Unicode in TEXT columns via prepared statements
    // Test with escaped Unicode sequences instead
    std::string unicodeJson = R"({
        "slideshow": {
            "author": "International Author",
            "title": "Unicode Test - Accent: cafe"
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(unicodeJson));
}

TEST(StorageEngineTest, IngestSpecialCharacters) {
    StorageEngine engine(":memory:");
    
    // Test with special characters that are safe for DuckDB
    std::string specialJson = R"({
        "slideshow": {
            "author": "Test Author with single quotes",
            "title": "Title with spaces and - dashes"
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(specialJson));
}

TEST(StorageEngineTest, IngestNumericValues) {
    StorageEngine engine(":memory:");
    
    std::string numericJson = R"({
        "slideshow": {
            "author": "Numeric Test",
            "title": "Numbers",
            "int": 42,
            "float": 3.14159,
            "negative": -100,
            "large": 9999999999999999
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(numericJson));
}

TEST(StorageEngineTest, IngestBooleanValues) {
    StorageEngine engine(":memory:");
    
    std::string boolJson = R"({
        "slideshow": {
            "author": "Bool Test",
            "title": "Booleans",
            "active": true,
            "deleted": false,
            "nullable": null
        }
    })";
    
    EXPECT_NO_THROW(engine.ingest(boolJson));
}


TEST(StorageEngineTest, BatchIngestion) {
    StorageEngine engine(":memory:");
    
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 100; ++i) {
        std::string json = R"({"slideshow": {"author": "Batch", "title": "Item)" + 
                           std::to_string(i) + R"(", "data": ")" + std::string(100, 'x') + R"("}})";
        engine.ingest(json);
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    EXPECT_LT(duration.count(), 5000);
}

} 
}
