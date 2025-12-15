#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <vector>
#include "core/RingBuffer.hpp"
#include "core/ThreadPool.hpp"
#include "data/StorageEngine.hpp"

std::atomic<bool> g_running{true};
std::atomic<size_t> g_bytes_ingested{0};
std::atomic<size_t> g_records_processed{0};

void consumerWorker(civic::RingBuffer<std::string>& buffer, civic::StorageEngine& storage) {
    std::string payload;
    while (g_running) {
        if (buffer.pop(payload)) {
            g_bytes_ingested += payload.size();
            storage.ingest(payload); 
            g_records_processed++;
        } else {
            std::this_thread::yield();
        }
    }
}

void mockProducer(civic::RingBuffer<std::string>& queue) {
    std::string mock_json = R"({
        "slideshow": {
            "author": "HighFreq Bot", 
            "title": "Benchmark Data",
            "date": "2025"
        }
    })";

    while(g_running) {
        if(!queue.push(mock_json)) {
            std::this_thread::yield(); 
        }

    }
}

void monitoringLoop() {
    auto last_time = std::chrono::steady_clock::now();
    size_t last_bytes = 0;
    size_t last_records = 0;

    std::cout << "\n[ SYSTEM STARTED : IN-MEMORY MODE ]\n" << std::endl;
    std::cout << std::left << std::setw(15) << "TIME" 
              << std::setw(15) << "NET (MB/s)" 
              << std::setw(15) << "DB (Rec/s)" 
              << std::setw(15) << "TOTAL" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        auto now = std::chrono::steady_clock::now();
        size_t current_bytes = g_bytes_ingested.load();
        size_t current_records = g_records_processed.load();

        double elapsed = std::chrono::duration<double>(now - last_time).count();
        double mb_s = (double)(current_bytes - last_bytes) / (1024 * 1024) / elapsed;
        double rec_s = (double)(current_records - last_records) / elapsed;

        std::cout << "\r" 
                  << std::left << std::setw(15) << "[RUNNING]" 
                  << std::fixed << std::setprecision(2) << std::setw(15) << mb_s 
                  << std::setw(15) << rec_s 
                  << std::setw(15) << current_records << std::flush;

        last_time = now;
        last_bytes = current_bytes;
        last_records = current_records;
    }
}

int main() {

    civic::StorageEngine storage(":memory:");

    civic::RingBuffer<std::string> queue(8192); 

    unsigned int num_workers = std::max(1u, std::thread::hardware_concurrency() - 2);
    civic::ThreadPool consumerPool(num_workers);
    consumerPool.setTask([&queue, &storage](){ 
        consumerWorker(queue, storage); 
    });
    
    std::cout << "[INIT] Workers: " << num_workers << " | Storage: RAM (Zero-Latency)" << std::endl;

    std::thread producerThread(mockProducer, std::ref(queue));

    monitoringLoop();

    if (producerThread.joinable()) producerThread.join();
    return 0;
}