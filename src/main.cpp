#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <vector>
#include <mutex>
#include <deque>
#include "core/RingBuffer.hpp"
#include "core/ThreadPool.hpp"
#include "data/StorageEngine.hpp"

std::atomic<bool> g_running{true};
std::atomic<size_t> g_bytes_ingested{0};
std::atomic<size_t> g_records_processed{0};
std::atomic<size_t> g_records_stored{0};
std::atomic<size_t> g_duplicates_skipped{0};

// Buffer pour afficher les derniers messages
std::mutex g_displayMutex;
std::deque<std::string> g_lastMessages;
constexpr size_t MAX_DISPLAY_MESSAGES = 8;

// Stocke aussi le JSON brut
std::deque<std::string> g_lastRawJson;

void addToDisplay(const std::string& author, const std::string& title, bool isDuplicate) {
    std::lock_guard<std::mutex> lock(g_displayMutex);
    std::string status = isDuplicate ? "[DUP]" : "[NEW]";
    std::string msg = status + " " + author + " - " + title;
    if (msg.length() > 60) msg = msg.substr(0, 57) + "...";
    
    g_lastMessages.push_front(msg);
    if (g_lastMessages.size() > MAX_DISPLAY_MESSAGES) {
        g_lastMessages.pop_back();
    }
}

void addRawJson(const std::string& json) {
    std::lock_guard<std::mutex> lock(g_displayMutex);
    g_lastRawJson.push_front(json);
    if (g_lastRawJson.size() > 3) {
        g_lastRawJson.pop_back();
    }
}

void consumerWorker(civic::RingBuffer<std::string>& buffer, civic::StorageEngine& storage) {
    std::string payload;
    while (g_running) {
        if (buffer.pop(payload)) {
            g_bytes_ingested += payload.size();
            
            // Stocker le JSON brut pour affichage
            addRawJson(payload);
            
            bool wasStored = storage.ingest(payload);
            if (wasStored) {
                g_records_stored++;
            } else {
                g_duplicates_skipped++;
            }
            g_records_processed++;
            
            // Ralentir pour voir ce qui passe (1ms entre chaque)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } else {
            std::this_thread::yield();
        }
    }
}

void mockProducer(civic::RingBuffer<std::string>& queue) {
    // Générateur de données variées
    std::vector<std::string> authors = {"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace"};
    std::vector<std::string> titles = {"API Call", "Sensor Data", "User Event", "Log Entry", "Metric", "Alert", "Transaction"};
    std::vector<std::string> types = {"info", "warning", "error", "debug", "trace"};
    
    size_t counter = 0;
    
    while(g_running) {
        // Générer un JSON unique à chaque fois
        std::string mock_json = R"({
        "slideshow": {
            "author": ")" + authors[counter % authors.size()] + R"(",
            "title": ")" + titles[counter % titles.size()] + " #" + std::to_string(counter) + R"(",
            "type": ")" + types[counter % types.size()] + R"(",
            "id": )" + std::to_string(counter) + R"(,
            "timestamp": )" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + R"(
        }
    })";
        
        if(!queue.push(mock_json)) {
            std::this_thread::yield(); 
        }
        
        counter++;
        // Ralentir un peu pour voir les données
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void monitoringLoop() {
    auto last_time = std::chrono::steady_clock::now();
    size_t last_bytes = 0;
    size_t last_records = 0;

    std::cout << "\n[ SYSTEM STARTED : DUCKDB PERSISTENT MODE ]\n" << std::endl;
    std::cout << std::string(70, '=') << std::endl;

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        auto now = std::chrono::steady_clock::now();
        size_t current_bytes = g_bytes_ingested.load();
        size_t current_records = g_records_processed.load();
        size_t stored = g_records_stored.load();
        size_t duplicates = g_duplicates_skipped.load();

        double elapsed = std::chrono::duration<double>(now - last_time).count();
        double mb_s = (double)(current_bytes - last_bytes) / (1024 * 1024) / elapsed;
        double rec_s = (double)(current_records - last_records) / elapsed;

        // Clear screen et afficher
        std::cout << "\033[2J\033[H"; // Clear terminal
        std::cout << std::string(70, '=') << std::endl;
        std::cout << "  HYPER-INGEST MONITOR" << std::endl;
        std::cout << std::string(70, '=') << std::endl;
        
        std::cout << "\n  STATS:" << std::endl;
        std::cout << "  ├─ Throughput:  " << std::fixed << std::setprecision(2) << mb_s << " MB/s  |  " << rec_s << " rec/s" << std::endl;
        std::cout << "  ├─ Processed:   " << current_records << std::endl;
        std::cout << "  ├─ Stored:      " << stored << " (unique)" << std::endl;
        std::cout << "  └─ Duplicates:  " << duplicates << " (skipped)" << std::endl;
        
        std::cout << "\n  LAST MESSAGES:" << std::endl;
        std::cout << "  " << std::string(50, '-') << std::endl;
        {
            std::lock_guard<std::mutex> lock(g_displayMutex);
            if (g_lastMessages.empty()) {
                std::cout << "  (waiting for data...)" << std::endl;
            }
            for (const auto& msg : g_lastMessages) {
                std::cout << "  " << msg << std::endl;
            }
        }
        std::cout << "  " << std::string(50, '-') << std::endl;
        
        // Afficher le JSON brut
        std::cout << "\n  RAW JSON DATA:" << std::endl;
        std::cout << "  " << std::string(66, '-') << std::endl;
        {
            std::lock_guard<std::mutex> lock(g_displayMutex);
            if (g_lastRawJson.empty()) {
                std::cout << "  (no data yet...)" << std::endl;
            } else {
                for (const auto& json : g_lastRawJson) {
                    // Afficher le JSON formaté (jusqu'à 300 chars)
                    std::string display = json;
                    // Remplacer les newlines par des espaces pour compacter
                    for (char& c : display) {
                        if (c == '\n' || c == '\r') c = ' ';
                    }
                    // Supprimer les espaces multiples
                    std::string clean;
                    bool lastWasSpace = false;
                    for (char c : display) {
                        if (c == ' ') {
                            if (!lastWasSpace) clean += c;
                            lastWasSpace = true;
                        } else {
                            clean += c;
                            lastWasSpace = false;
                        }
                    }
                    if (clean.length() > 66) clean = clean.substr(0, 63) + "...";
                    std::cout << "  " << clean << std::endl;
                }
            }
        }
        std::cout << "  " << std::string(66, '-') << std::endl;
        
        std::cout << "\n  Press Ctrl+C to stop" << std::endl;

        last_time = now;
        last_bytes = current_bytes;
        last_records = current_records;
    }
}

int main() {

    civic::StorageEngine storage("hyper_ingest.duckdb");
    
    // Callback pour afficher les messages traités
    storage.setDisplayCallback(addToDisplay);

    civic::RingBuffer<std::string> queue(8192); 

    unsigned int num_workers = std::max(1u, std::thread::hardware_concurrency() - 2);
    civic::ThreadPool consumerPool(num_workers);
    consumerPool.setTask([&queue, &storage](){ 
        consumerWorker(queue, storage); 
    });
    
    std::cout << "[INIT] Workers: " << num_workers << " | Storage: DuckDB (Persistent)" << std::endl;

    std::thread producerThread(mockProducer, std::ref(queue));

    monitoringLoop();

    if (producerThread.joinable()) producerThread.join();
    return 0;
}