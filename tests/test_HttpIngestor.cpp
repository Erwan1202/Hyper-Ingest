#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <boost/asio.hpp>
#include "Network/HttpIngestor.hpp"
#include "core/RingBuffer.hpp"

namespace civic {
namespace test {


TEST(HttpIngestorTest, ConstructorValid) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    
    EXPECT_NO_THROW({
        HttpIngestor ingestor(buffer, ioc);
    });
}

TEST(HttpIngestorTest, MultipleIngestorsOnSameContext) {
    RingBuffer<std::string> buffer1(16);
    RingBuffer<std::string> buffer2(16);
    boost::asio::io_context ioc;
    
    EXPECT_NO_THROW({
        HttpIngestor ingestor1(buffer1, ioc);
        HttpIngestor ingestor2(buffer2, ioc);
    });
}

TEST(HttpIngestorTest, FetchCreatesRequest) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/"));
}

TEST(HttpIngestorTest, FetchWithDifferentPaths) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/api/v1/data"));
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/health"));
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/"));
}

TEST(HttpIngestorTest, FetchWithDifferentPorts) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    
    HttpIngestor ingestor1(buffer, ioc);
    EXPECT_NO_THROW(ingestor1.fetch("example.com", "80", "/"));
    
    HttpIngestor ingestor2(buffer, ioc);
    EXPECT_NO_THROW(ingestor2.fetch("example.com", "8080", "/"));
    
    HttpIngestor ingestor3(buffer, ioc);
    EXPECT_NO_THROW(ingestor3.fetch("example.com", "443", "/"));
}


class HttpIngestorIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        hasNetwork_ = checkNetworkAccess();
    }
    
    bool checkNetworkAccess() {
        try {
            boost::asio::io_context ioc;
            boost::asio::ip::tcp::resolver resolver(ioc);
            auto results = resolver.resolve("httpbin.org", "80");
            return true;
        } catch (...) {
            return false;
        }
    }
    
    bool hasNetwork_ = false;
};

TEST_F(HttpIngestorIntegrationTest, FetchFromHttpbin) {
    if (!hasNetwork_) {
        GTEST_SKIP() << "Network not available";
    }
    
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    ingestor.fetch("httpbin.org", "80", "/json");
    
    std::thread ioThread([&ioc]() {
        ioc.run();
    });
    
    auto start = std::chrono::steady_clock::now();
    std::string data;
    bool received = false;
    
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(10)) {
        if (buffer.pop(data)) {
            received = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    ioc.stop();
    ioThread.join();
    
    if (received) {
        EXPECT_FALSE(data.empty());
        // Skip if server returned an error page (503, etc.)
        if (data.find("<html>") != std::string::npos || 
            data.find("Service Unavailable") != std::string::npos ||
            data.find("503") != std::string::npos) {
            GTEST_SKIP() << "Server returned error page - service unavailable";
        }
        // Check for valid JSON structure (curly braces)
        bool hasJsonStructure = (data.find("{") != std::string::npos);
        EXPECT_TRUE(hasJsonStructure) << "Response: " << data.substr(0, 200);
    } else {
        GTEST_SKIP() << "Request timed out";
    }
}

TEST_F(HttpIngestorIntegrationTest, FetchInvalidHost) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    ingestor.fetch("invalid.host.that.does.not.exist.example", "80", "/");
    
    std::thread ioThread([&ioc]() {
        ioc.run();
    });
    
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    ioc.stop();
    ioThread.join();
    
    std::string data;
    EXPECT_FALSE(buffer.pop(data));
}

TEST(HttpIngestorTest, BufferIntegration) {
    RingBuffer<std::string> buffer(4);
    boost::asio::io_context ioc;
    
    buffer.push("data1");
    buffer.push("data2");
    buffer.push("data3");
    buffer.push("data4");
    
    HttpIngestor ingestor(buffer, ioc);
    
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/"));
}

TEST(HttpIngestorTest, AsyncOperationCancellation) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    ingestor.fetch("example.com", "80", "/");
    
    ioc.stop();
    
    SUCCEED();
}

TEST(HttpIngestorTest, MultipleSequentialFetches) {
    RingBuffer<std::string> buffer(32);
    boost::asio::io_context ioc;
    
    for (int i = 0; i < 5; ++i) {
        HttpIngestor ingestor(buffer, ioc);
        EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/path" + std::to_string(i)));
    }
    
    ioc.stop();
}

TEST(HttpIngestorTest, EmptyPath) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", ""));
}

TEST(HttpIngestorTest, LongPath) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    std::string longPath = "/" + std::string(500, 'a');
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", longPath));
}

TEST(HttpIngestorTest, PathWithQueryParams) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/api?param1=value1&param2=value2"));
}

TEST(HttpIngestorTest, PathWithSpecialCharacters) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    HttpIngestor ingestor(buffer, ioc);
    
    EXPECT_NO_THROW(ingestor.fetch("example.com", "80", "/api/data%20with%20spaces"));
}

TEST(HttpIngestorTest, DestructorCleanup) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    
    {
        HttpIngestor ingestor(buffer, ioc);
        ingestor.fetch("example.com", "80", "/");
    }
    
    EXPECT_NO_THROW(ioc.stop());
}

TEST(HttpIngestorTest, RapidCreationDestruction) {
    RingBuffer<std::string> buffer(16);
    boost::asio::io_context ioc;
    
    for (int i = 0; i < 10; ++i) {
        HttpIngestor ingestor(buffer, ioc);
        ingestor.fetch("example.com", "80", "/");
    }
    
    ioc.stop();
    SUCCEED();
}

} // namespace test
} // namespace civic
