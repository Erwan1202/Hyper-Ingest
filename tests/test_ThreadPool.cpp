#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>
#include "core/ThreadPool.hpp"

namespace civic {
namespace test {


TEST(ThreadPoolTest, ConstructorDefault) {
    EXPECT_NO_THROW({
        ThreadPool pool;
        pool.stop();
    });
}

TEST(ThreadPoolTest, ConstructorWithThreadCount) {
    EXPECT_NO_THROW({
        ThreadPool pool(4);
        pool.stop();
    });
}

TEST(ThreadPoolTest, ConstructorSingleThread) {
    EXPECT_NO_THROW({
        ThreadPool pool(1);
        pool.stop();
    });
}


TEST(ThreadPoolTest, TaskExecution) {
    ThreadPool pool(2);
    std::atomic<int> counter{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&counter, &shouldRun]() {
        if (shouldRun.load()) {
            counter.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    shouldRun = false;
    pool.stop();
    
    EXPECT_GT(counter.load(), 0);
}

TEST(ThreadPoolTest, MultipleThreadsExecuting) {
    const size_t numThreads = 4;
    ThreadPool pool(numThreads);
    
    std::atomic<int> counter{0};
    std::set<std::thread::id> threadIds;
    std::mutex idMutex;
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        if (shouldRun.load()) {
            {
                std::lock_guard<std::mutex> lock(idMutex);
                threadIds.insert(std::this_thread::get_id());
            }
            counter.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    shouldRun = false;
    pool.stop();
    
    EXPECT_GT(threadIds.size(), 1);
    EXPECT_LE(threadIds.size(), numThreads);
}


TEST(ThreadPoolTest, StopWithoutTask) {
    ThreadPool pool(2);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    EXPECT_NO_THROW(pool.stop());
}

TEST(ThreadPoolTest, DoubleStopSafe) {
    ThreadPool pool(2);
    
    pool.stop();
    EXPECT_NO_THROW(pool.stop());
}

TEST(ThreadPoolTest, DestructorStops) {
    std::atomic<bool> taskRunning{true};
    
    {
        ThreadPool pool(2);
        pool.setTask([&taskRunning]() {
            if (taskRunning) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        });
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        taskRunning = false;
    }
    
    SUCCEED();
}


TEST(ThreadPoolTest, ConcurrentTaskExecution) {
    const size_t numThreads = 4;
    ThreadPool pool(numThreads);
    
    std::atomic<int> activeCount{0};
    std::atomic<int> maxConcurrent{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        if (!shouldRun.load()) return;
        
        int current = activeCount.fetch_add(1) + 1;
        
        int prevMax = maxConcurrent.load();
        while (current > prevMax && !maxConcurrent.compare_exchange_weak(prevMax, current)) {
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        activeCount.fetch_sub(1);
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    shouldRun = false;
    pool.stop();
    
    EXPECT_GE(maxConcurrent.load(), 1);
}


TEST(ThreadPoolTest, TaskCanBeChanged) {
    ThreadPool pool(2);
    
    std::atomic<int> firstTaskCount{0};
    std::atomic<int> secondTaskCount{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        if (shouldRun.load()) {
            firstTaskCount.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    pool.setTask([&]() {
        if (shouldRun.load()) {
            secondTaskCount.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    shouldRun = false;
    pool.stop();
    
    EXPECT_GT(firstTaskCount.load(), 0);
    EXPECT_GT(secondTaskCount.load(), 0);
}

TEST(ThreadPoolTest, StressTest) {
    const size_t numThreads = 8;
    ThreadPool pool(numThreads);
    
    std::atomic<uint64_t> operationCount{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        while (shouldRun.load(std::memory_order_relaxed)) {
            operationCount.fetch_add(1, std::memory_order_relaxed);
            volatile int x = 0;
            for (int i = 0; i < 100; ++i) {
                x += i;
            }
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    shouldRun = false;
    pool.stop();
    
    EXPECT_GT(operationCount.load(), 1000);
}

TEST(ThreadPoolTest, RapidStartStop) {
    for (int i = 0; i < 10; ++i) {
        ThreadPool pool(4);
        std::atomic<int> counter{0};
        
        pool.setTask([&counter]() {
            counter.fetch_add(1);
        });
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        pool.stop();
    }
    
    SUCCEED();
}


TEST(ThreadPoolTest, EmptyTaskDoesNotCrash) {
    ThreadPool pool(2);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    pool.stop();
    SUCCEED();
}

TEST(ThreadPoolTest, LargeNumberOfThreads) {
    const size_t numThreads = 16;
    ThreadPool pool(numThreads);
    
    std::atomic<int> counter{0};
    std::atomic<bool> shouldRun{true};
    
    pool.setTask([&]() {
        if (shouldRun.load()) {
            counter.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    shouldRun = false;
    pool.stop();
    
    EXPECT_GT(counter.load(), 0);
}

} 
} 
