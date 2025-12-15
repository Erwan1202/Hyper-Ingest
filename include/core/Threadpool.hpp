#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>

namespace civic {

    class ThreadPool {
    public:
        explicit ThreadPool(size_t numThreads = std::thread::hardware_concurrency()) 
            : stop_(false) 
        {
            workers_.reserve(numThreads);
            for (size_t i = 0; i < numThreads; ++i) {
                workers_.emplace_back([this] {
                    this->workerLoop();
                });
            }
        }

        ~ThreadPool() {
            stop();
        }

        void stop() {
            if (!stop_.exchange(true)) {
                for (std::thread &worker : workers_) {
                    if (worker.joinable()) {
                        worker.join();
                    }
                }
            }
        }

        void setTask(std::function<void()> task) {
            task_ = task;
        }

    private:
        void workerLoop() {
            while (!stop_.load(std::memory_order_relaxed)) {
                if (task_) {
                    task_();
                } else {
                    std::this_thread::yield(); 
                }
            }
        }

        std::vector<std::thread> workers_;
        std::atomic<bool> stop_;
        std::function<void()> task_;
    };
}