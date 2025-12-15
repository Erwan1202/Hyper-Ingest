#pragma once

#include <string>
#include <memory>
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include "core/RingBuffer.hpp"

namespace civic {
    namespace beast = boost::beast;
    namespace http = beast::http;
    namespace net = boost::asio;
    using tcp = net::ip::tcp;

    class HttpIngestor {
    public:
        explicit HttpIngestor(RingBuffer<std::string>& buffer, net::io_context& ioc);

        void fetch(const std::string& host, const std::string& port, const std::string& target);

    private:
        void onResolve(beast::error_code ec, tcp::resolver::results_type results);
        void onConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type);
        void onWrite(beast::error_code ec, std::size_t bytes_transferred);
        void onRead(beast::error_code ec, std::size_t bytes_transferred);

        RingBuffer<std::string>& buffer_;
        tcp::resolver resolver_;
        beast::tcp_stream stream_;
        beast::flat_buffer responseBuffer_;
        http::request<http::empty_body> req_;
        http::response<http::string_body> res_;
    };
}