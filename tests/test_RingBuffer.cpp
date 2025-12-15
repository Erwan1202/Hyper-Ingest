#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <set>
#include "core/RingBuffer.hpp"

namespace civic {
namespace test {


TEST(RingBufferTest, ConstructorWithPowerOfTwo) {
    EXPECT_NO_THROW(RingBuffer<int> buffer(2));
    EXPECT_NO_THROW(RingBuffer<int> buffer(4));
    EXPECT_NO_THROW(RingBuffer<int> buffer(8));
    EXPECT_NO_THROW(RingBuffer<int> buffer(16));
    EXPECT_NO_THROW(RingBuffer<int> buffer(1024));
}

TEST(RingBufferTest, PushAndPopSingleElement) {
    RingBuffer<int> buffer(4);
    
    EXPECT_TRUE(buffer.push(42));
    
    int value = 0;
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, 42);
}

TEST(RingBufferTest, PopFromEmptyBufferReturnsFalse) {
    RingBuffer<int> buffer(4);
    
    int value = 0;
    EXPECT_FALSE(buffer.pop(value));
}

TEST(RingBufferTest, PushMultipleElements) {
    RingBuffer<int> buffer(8);
    
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(buffer.push(i * 10));
    }
    
    for (int i = 0; i < 5; ++i) {
        int value = -1;
        EXPECT_TRUE(buffer.pop(value));
        EXPECT_EQ(value, i * 10);
    }
}

TEST(RingBufferTest, BufferFullReturnsFalse) {
    RingBuffer<int> buffer(4);
    
    EXPECT_TRUE(buffer.push(1));
    EXPECT_TRUE(buffer.push(2));
    EXPECT_TRUE(buffer.push(3));
    EXPECT_TRUE(buffer.push(4));
    
    EXPECT_FALSE(buffer.push(5));
}

TEST(RingBufferTest, FIFO_Order) {
    RingBuffer<std::string> buffer(8);
    
    buffer.push("first");
    buffer.push("second");
    buffer.push("third");
    
    std::string value;
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, "first");
    
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, "second");
    
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, "third");
}

TEST(RingBufferTest, WrapAround) {
    RingBuffer<int> buffer(4);
    
    for (int cycle = 0; cycle < 3; ++cycle) {
        for (int i = 0; i < 4; ++i) {
            EXPECT_TRUE(buffer.push(cycle * 100 + i));
        }
        
        for (int i = 0; i < 4; ++i) {
            int value;
            EXPECT_TRUE(buffer.pop(value));
            EXPECT_EQ(value, cycle * 100 + i);
        }
    }
}


TEST(RingBufferTest, SingleProducerSingleConsumer) {
    RingBuffer<int> buffer(1024);
    const int numItems = 10000;
    std::atomic<int> producedCount{0};
    std::atomic<int> consumedCount{0};
    std::vector<int> consumed;
    std::mutex consumedMutex;
    
    std::thread producer([&]() {
        for (int i = 0; i < numItems; ++i) {
            while (!buffer.push(i)) {
                std::this_thread::yield();
            }
            producedCount++;
        }
    });
    
    std::thread consumer([&]() {
        while (consumedCount < numItems) {
            int value;
            if (buffer.pop(value)) {
                std::lock_guard<std::mutex> lock(consumedMutex);
                consumed.push_back(value);
                consumedCount++;
            } else {
                std::this_thread::yield();
            }
        }
    });
    
    producer.join();
    consumer.join();
    
    EXPECT_EQ(consumed.size(), numItems);
    
    for (int i = 0; i < numItems; ++i) {
        EXPECT_EQ(consumed[i], i);
    }
}

TEST(RingBufferTest, MultipleProducersSingleConsumer) {
    RingBuffer<int> buffer(1024);
    const int numProducers = 4;
    const int itemsPerProducer = 2500;
    const int totalItems = numProducers * itemsPerProducer;
    
    std::atomic<int> consumedCount{0};
    std::set<int> consumedSet;
    std::mutex setMutex;
    
    std::vector<std::thread> producers;
    for (int p = 0; p < numProducers; ++p) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < itemsPerProducer; ++i) {
                int value = p * itemsPerProducer + i;
                while (!buffer.push(value)) {
                    std::this_thread::yield();
                }
            }
        });
    }
    
    std::thread consumer([&]() {
        while (consumedCount < totalItems) {
            int value;
            if (buffer.pop(value)) {
                std::lock_guard<std::mutex> lock(setMutex);
                consumedSet.insert(value);
                consumedCount++;
            } else {
                std::this_thread::yield();
            }
        }
    });
    
    for (auto& t : producers) {
        t.join();
    }
    consumer.join();
    
    EXPECT_EQ(consumedSet.size(), totalItems);
}

TEST(RingBufferTest, SingleProducerMultipleConsumers) {
    RingBuffer<int> buffer(1024);
    const int numConsumers = 4;
    const int totalItems = 10000;
    
    std::atomic<int> producedCount{0};
    std::atomic<int> consumedCount{0};
    std::set<int> consumedSet;
    std::mutex setMutex;
    
    std::thread producer([&]() {
        for (int i = 0; i < totalItems; ++i) {
            while (!buffer.push(i)) {
                std::this_thread::yield();
            }
            producedCount++;
        }
    });
    
    std::vector<std::thread> consumers;
    for (int c = 0; c < numConsumers; ++c) {
        consumers.emplace_back([&]() {
            while (consumedCount < totalItems) {
                int value;
                if (buffer.pop(value)) {
                    std::lock_guard<std::mutex> lock(setMutex);
                    consumedSet.insert(value);
                    consumedCount++;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    
    producer.join();
    for (auto& t : consumers) {
        t.join();
    }
    
    // All items should be received
    EXPECT_EQ(consumedSet.size(), totalItems);
}


TEST(RingBufferTest, StringType) {
    RingBuffer<std::string> buffer(8);
    
    std::string longString(1000, 'x');
    EXPECT_TRUE(buffer.push(longString));
    EXPECT_TRUE(buffer.push("short"));
    EXPECT_TRUE(buffer.push(""));
    
    std::string value;
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, longString);
    
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, "short");
    
    EXPECT_TRUE(buffer.pop(value));
    EXPECT_EQ(value, "");
}


TEST(RingBufferTest, StressTest) {
    RingBuffer<int> buffer(256);
    const int numOperations = 100000;
    std::atomic<bool> running{true};
    std::atomic<int> totalPushed{0};
    std::atomic<int> totalPopped{0};
    
    std::thread producer([&]() {
        for (int i = 0; i < numOperations; ++i) {
            while (!buffer.push(i) && running) {
                std::this_thread::yield();
            }
            totalPushed++;
        }
        running = false;
    });
    
    std::thread consumer([&]() {
        int value;
        while (running || totalPopped < totalPushed) {
            if (buffer.pop(value)) {
                totalPopped++;
            } else {
                std::this_thread::yield();
            }
        }
    });
    
    producer.join();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    running = false;
    consumer.join();
    
    EXPECT_EQ(totalPushed.load(), totalPopped.load());
}

}
}
