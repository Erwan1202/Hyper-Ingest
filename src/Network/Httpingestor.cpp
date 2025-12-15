#include "Network/HttpIngestor.hpp"
#include <iostream>

namespace civic {

    HttpIngestor::HttpIngestor(RingBuffer<std::string>& buffer, net::io_context& ioc)
        : buffer_(buffer), resolver_(ioc), stream_(ioc) 
    {
    }

    void HttpIngestor::fetch(const std::string& host, const std::string& port, const std::string& target) {
        req_.version(11);
        req_.method(http::verb::get);
        req_.target(target);
        req_.set(http::field::host, host);
        req_.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);

        resolver_.async_resolve(
            host, 
            port,
            beast::bind_front_handler(&HttpIngestor::onResolve, this)
        );
    }

    void HttpIngestor::onResolve(beast::error_code ec, tcp::resolver::results_type results) {
        if(ec) {
            std::cerr << "[NET] Resolve failed: " << ec.message() << std::endl;
            return;
        }

        stream_.expires_after(std::chrono::seconds(30));
        stream_.async_connect(
            results,
            beast::bind_front_handler(&HttpIngestor::onConnect, this)
        );
    }

    void HttpIngestor::onConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type) {
        if(ec) {
            std::cerr << "[NET] Connect failed: " << ec.message() << std::endl;
            return;
        }

        stream_.expires_after(std::chrono::seconds(30));
        http::async_write(stream_, req_,
            beast::bind_front_handler(&HttpIngestor::onWrite, this)
        );
    }

    void HttpIngestor::onWrite(beast::error_code ec, std::size_t bytes_transferred) {
        if(ec) {
            std::cerr << "[NET] Write failed: " << ec.message() << std::endl;
            return;
        }

        http::async_read(stream_, responseBuffer_, res_,
            beast::bind_front_handler(&HttpIngestor::onRead, this)
        );
    }

    void HttpIngestor::onRead(beast::error_code ec, std::size_t bytes_transferred) {
        if(ec) {
            std::cerr << "[NET] Read failed: " << ec.message() << std::endl;
            return;
        }

        if (!buffer_.push(res_.body())) {
            std::cerr << "[NET] RingBuffer FULL! Dropping packet." << std::endl;
        } else {
            std::cout << "[NET] Ingested " << bytes_transferred << " bytes." << std::endl;
        }

        beast::error_code code;
        stream_.socket().shutdown(tcp::socket::shutdown_both, code);
    }
}