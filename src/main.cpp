#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include "core/RingBuffer.hpp"
#include "core/Threadpool.hpp"
#include "Network/HttpIngestor.hpp"

std::atomic<bool> g_running{true};

void consumerTask(civic::RingBuffer<std::string>& buffer) {
    std::string data;
    while (g_running) {
        if (buffer.pop(data)) {
            std::cout << "\n[CONSUMER] Received Payload (" << data.size() << " bytes):\n" 
                      << data.substr(0, 100) << "...\n[END PREVIEW]\n" << std::endl;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

int main() {
    std::cout << "[SYSTEM] Starting CivicCore Integration Test..." << std::endl;

    civic::RingBuffer<std::string> buffer(16); 

    civic::ThreadPool pool(2);
    pool.setTask([&buffer](){ 
        consumerTask(buffer); 
    });
    std::cout << "[SYSTEM] ThreadPool (Consumers) started." << std::endl;

    boost::asio::io_context ioc;
    civic::HttpIngestor ingestor(buffer, ioc);

    std::cout << "[SYSTEM] Launching HTTP Request..." << std::endl;
    ingestor.fetch("httpbin.org", "80", "/json");

    std::thread ioThread([&ioc](){
        ioc.run();
    });

    std::this_thread::sleep_for(std::chrono::seconds(3));
    
    g_running = false;
    ioThread.join();
    
    std::cout << "[SYSTEM] Test Finished." << std::endl;
    return 0;
}