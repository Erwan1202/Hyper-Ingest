#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "core/RingBuffer.hpp"
#include "core/ThreadPool.hpp"
#include "data/StorageEngine.hpp"
#include "Network/HttpIngestor.hpp"

namespace civic {
namespace test {

TEST(IntegrationTest, DataPipeline) {
    RingBuffer<std::string> buffer(64);
    StorageEngine storage(":memory:");
    auto con = storage.createConnection();
    ThreadPool pool(2);
    
    std::atomic<int> processedCount{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        while (shouldRun.load()) {
            std::string data;
            if (buffer.pop(data)) {
                storage.ingest(*con, data);
                processedCount.fetch_add(1);
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    });
    
    const int numItems = 50;
    for (int i = 0; i < numItems; ++i) {
        std::string json = R"({
            "slideshow": {
                "author": "Pipeline Test )" + std::to_string(i) + R"(",
                "title": "Integration Item )" + std::to_string(i) + R"("
            }
        })";
        
        while (!buffer.push(json)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    shouldRun = false;
    pool.stop();
    
    EXPECT_EQ(processedCount.load(), numItems);
}

TEST(IntegrationTest, MultipleProducerPipeline) {
    RingBuffer<std::string> buffer(256);
    std::atomic<int> producedCount{0};
    std::atomic<int> consumedCount{0};
    std::atomic<bool> shouldRun{true};
    
    const int numProducers = 4;
    const int itemsPerProducer = 100;
    const int totalItems = numProducers * itemsPerProducer;
    
    ThreadPool pool(2);
    pool.setTask([&]() {
        while (shouldRun.load() || consumedCount.load() < producedCount.load()) {
            std::string data;
            if (buffer.pop(data)) {
                consumedCount.fetch_add(1);
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        }
    });
    
    std::vector<std::thread> producers;
    for (int p = 0; p < numProducers; ++p) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < itemsPerProducer; ++i) {
                std::string json = R"({"producer": )" + std::to_string(p) + 
                                   R"(, "item": )" + std::to_string(i) + "}";
                while (!buffer.push(json)) {
                    std::this_thread::yield();
                }
                producedCount.fetch_add(1);
            }
        });
    }
    
    for (auto& t : producers) {
        t.join();
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    shouldRun = false;
    pool.stop();
    
    EXPECT_EQ(producedCount.load(), totalItems);
    EXPECT_EQ(consumedCount.load(), totalItems);
}
// Gros caca Made In Moi (ca s'appelle la fatigue chérie :D)
TEST(IntegrationTest, HighContention) {
    RingBuffer<int> buffer(128);
    std::atomic<int> totalProduced{0};
    std::atomic<int> totalConsumed{0};
    std::atomic<bool> producing{true};
    
    const int numProducerThreads = 4;
    const int numConsumerThreads = 4;
    const int itemsPerProducer = 1000;
    
    std::vector<std::thread> consumers;
    for (int c = 0; c < numConsumerThreads; ++c) {
        consumers.emplace_back([&]() {
            while (producing.load() || totalConsumed.load() < totalProduced.load()) {
                int value;
                if (buffer.pop(value)) {
                    totalConsumed.fetch_add(1);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    
    std::vector<std::thread> producers;
    for (int p = 0; p < numProducerThreads; ++p) {
        producers.emplace_back([&]() {
            for (int i = 0; i < itemsPerProducer; ++i) {
                while (!buffer.push(i)) {
                    std::this_thread::yield();
                }
                totalProduced.fetch_add(1);
            }
        });
    }
    
    for (auto& t : producers) {
        t.join();
    }
    producing = false;
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    for (auto& t : consumers) {
        t.join();
    }
    
    EXPECT_EQ(totalProduced.load(), numProducerThreads * itemsPerProducer);
    EXPECT_EQ(totalConsumed.load(), totalProduced.load());
}

TEST(IntegrationTest, StorageConcurrency) {
    StorageEngine storage(":memory:");
    auto con = storage.createConnection();
    std::atomic<int> writeCount{0};
    std::atomic<int> readCount{0};
    std::atomic<bool> running{true};
    
    const int numWrites = 100;
    
    std::thread writer([&]() {
        for (int i = 0; i < numWrites; ++i) {
            std::string json = R"({"slideshow": {"author": "Writer", "title": "Item)" + 
                               std::to_string(i) + R"("}})";
            storage.ingest(*con, json);
            writeCount.fetch_add(1);
        }
    });
    
    std::vector<std::thread> readers;
    for (int r = 0; r < 3; ++r) {
        readers.emplace_back([&]() {
            while (running.load()) {
                storage.query(*con, "SELECT COUNT(*) FROM ingest_logs");
                readCount.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        });
    }
    
    writer.join();
    running = false;
    
    for (auto& t : readers) {
        t.join();
    }
    
    EXPECT_EQ(writeCount.load(), numWrites);
    EXPECT_GT(readCount.load(), 0);
}


TEST(IntegrationTest, SimulatedHttpIngestion) {
    RingBuffer<std::string> buffer(64);
    StorageEngine storage(":memory:");
    auto con = storage.createConnection();
    ThreadPool pool(2);
    
    std::atomic<int> ingestedCount{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        while (shouldRun.load()) {
            std::string data;
            if (buffer.pop(data)) {
                storage.ingest(*con, data);
                ingestedCount.fetch_add(1);
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    });
    
    std::vector<std::string> simulatedResponses = {
        R"({"slideshow": {"author": "API 1", "title": "Response 1", "data": [1,2,3]}})",
        R"({"slideshow": {"author": "API 2", "title": "Response 2", "nested": {"key": "value"}}})",
        R"({"slideshow": {"author": "API 3", "title": "Response 3", "count": 42}})",
        R"({"slideshow": {"author": "API 4", "title": "Response 4", "active": true}})",
        R"({"slideshow": {"author": "API 5", "title": "Response 5", "items": ["a", "b", "c"]}})"
    };
    
    for (const auto& response : simulatedResponses) {
        while (!buffer.push(response)) {
            std::this_thread::yield();
        }
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    shouldRun = false;
    pool.stop();
    
    EXPECT_EQ(ingestedCount.load(), static_cast<int>(simulatedResponses.size()));
}

TEST(IntegrationTest, BufferFullRecovery) {
    RingBuffer<std::string> buffer(4); 
    std::atomic<int> droppedCount{0};
    std::atomic<int> processedCount{0};
    std::atomic<bool> consuming{false};
    std::atomic<bool> shouldRun{true};
    
    std::thread producer([&]() {
        for (int i = 0; i < 20; ++i) {
            std::string data = "item" + std::to_string(i);
            if (!buffer.push(data)) {
                droppedCount.fetch_add(1);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    std::thread consumer([&]() {
        consuming = true;
        while (shouldRun.load()) {
            std::string data;
            if (buffer.pop(data)) {
                processedCount.fetch_add(1);
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }
    });
    
    producer.join();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    shouldRun = false;
    consumer.join();
    
    EXPECT_GT(droppedCount.load(), 0); 
    EXPECT_GT(processedCount.load(), 0); 
    EXPECT_EQ(droppedCount.load() + processedCount.load(), 20);
}


TEST(IntegrationTest, ThroughputBenchmark) {
    RingBuffer<std::string> buffer(1024);
    std::atomic<int> count{0};
    std::atomic<bool> running{true};
    
    const auto testDuration = std::chrono::seconds(1);
    
    std::thread consumer([&]() {
        while (running.load()) {
            std::string data;
            if (buffer.pop(data)) {
                count.fetch_add(1);
            }
        }
    });
    
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < testDuration) {
        buffer.push("benchmark data payload");
    }
    
    running = false;
    consumer.join();
    
    EXPECT_GT(count.load(), 10000);
    
    std::cout << "[BENCHMARK] Processed " << count.load() << " items in 1 second" << std::endl;
}

TEST(IntegrationTest, LatencyTest) {
    RingBuffer<std::chrono::steady_clock::time_point> buffer(1024);
    std::vector<long long> latencies;
    std::mutex latencyMutex;
    std::atomic<bool> running{true};
    
    const int numSamples = 1000;
    
    std::thread consumer([&]() {
        while (running.load() || latencies.size() < numSamples) {
            std::chrono::steady_clock::time_point sendTime;
            if (buffer.pop(sendTime)) {
                auto receiveTime = std::chrono::steady_clock::now();
                auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    receiveTime - sendTime).count();
                
                std::lock_guard<std::mutex> lock(latencyMutex);
                latencies.push_back(latency);
            }
        }
    });
    
    for (int i = 0; i < numSamples; ++i) {
        while (!buffer.push(std::chrono::steady_clock::now())) {
            std::this_thread::yield();
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    running = false;
    consumer.join();
    
    if (!latencies.empty()) {
        long long sum = 0;
        for (auto l : latencies) sum += l;
        long long avgNs = sum / latencies.size();
        
        std::cout << "[BENCHMARK] Average latency: " << avgNs << " ns" << std::endl;
        
        EXPECT_LT(avgNs, 1000000);
    }
}

} 
} 
