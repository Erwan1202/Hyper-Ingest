#pragma once

#include <vector>
#include <thread>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <queue>

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
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stop_ = true;
            }
            cv_.notify_all();
            for (std::thread &worker : workers_) {
                if (worker.joinable()) {
                    worker.join();
                }
            }
        }

        void enqueue(std::function<void()> task) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                tasks_.push(std::move(task));
            }
            cv_.notify_one();
        }

        // Set a repeating task for all workers to execute continuously
        void setTask(std::function<void()> task) {
            task_ = std::move(task);
            cv_.notify_all();
        }

    private:
        void workerLoop() {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    cv_.wait(lock, [this] {
                        return stop_ || !tasks_.empty() || task_;
                    });
                    
                    // Exit if stopped
                    if (stop_) {
                        return;
                    }
                    
                    if (!tasks_.empty()) {
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    } else if (task_) {
                        task = task_;
                    }
                }
                if (task) {
                    task();
                }
            }
        }

        std::vector<std::thread> workers_;
        std::queue<std::function<void()>> tasks_;
        std::function<void()> task_; 
        std::mutex mutex_;
        std::condition_variable cv_;
        bool stop_;
    };
}