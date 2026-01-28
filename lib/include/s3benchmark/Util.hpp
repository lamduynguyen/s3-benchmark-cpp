//
// Created by Maximilian Kuschewski on 07.06.20.
//

#ifndef _S3BENCHMARK_UTIL_HPP
#define _S3BENCHMARK_UTIL_HPP

#include <thread>
#include <random>
#include <iostream>

#include <curl/curl.h>

namespace s3benchmark {
    namespace hardware {
        inline size_t thread_count() noexcept {
            return std::max(std::thread::hardware_concurrency(), 1u);
        }
    }

    namespace units {
        inline static const size_t kib = 1024;
        inline static const size_t mib = 1024 * kib;
        inline static const size_t gib = 1024 * mib;

        inline static const size_t ns_per_sec = 1000000000UL;
        inline static const size_t ms_per_sec = 1000;
        inline static const size_t ms_per_min = 60 * ms_per_sec;
    }

    namespace random {
        template<typename T>
        inline T in_range(T min, T max) {
            thread_local static std::mt19937 engine {std::random_device{}()};
            thread_local static std::uniform_int_distribution<T> dist(min, max);
            return dist(engine);
        };
    }

    namespace stream {
        class VoidBuffer : public std::streambuf {
        public:
            inline int overflow(int c) override {
                return c;
            }
        };
    }

    namespace format {
        // Not using c++ 20 std::format
        // from https://stackoverflow.com/questions/2342162/stdstring-formatting-like-sprintf
        template<typename ... Args>
        inline std::string string_format(const std::string& format, Args ... args) {
            size_t size = snprintf( nullptr, 0, format.c_str(), args ... ) + 1; // Extra space for '\0'
            if( size <= 0 ){ throw std::runtime_error( "Error during formatting." ); }
            std::unique_ptr<char[]> buf( new char[ size ] );
            snprintf( buf.get(), size, format.c_str(), args ... );
            return std::string( buf.get(), buf.get() + size - 1 ); // We don't want the '\0' inside
        }

        inline std::string byte_format(double bytes) {
            if (bytes >= units::gib) {
                return string_format("%.f GiB", bytes / units::gib);
            }
            if (bytes >= units::mib) {
                return string_format("%.f MiB", bytes / units::mib);
            }
            if (bytes >= units::kib) {
                return string_format("%.f KiB", bytes / units::kib);
            }
            return string_format("%d", bytes);
        }
    }

    namespace http {
        inline size_t curl_callback(char *body, size_t size_mult, size_t nmemb, void *userdata) {
            auto size = size_mult * nmemb;
            static_cast<std::string*>(userdata)->append(body, size);
            return size;
        }

        inline std::string curl_get(const std::string &url, size_t timeout_ms = 1000) {
            CURL *curl = curl_easy_init();
            if (!curl) {
                throw std::runtime_error("Could not initialize curl client.");
            }
            CURLcode res;
            std::string body;
            curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, &curl_callback);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, timeout_ms);
            res = curl_easy_perform(curl);
            if (res == 0) {
                return body;
            } else {
                throw std::runtime_error("Failed fetching instance id.");
            }
        }
    }
}

#endif //_S3BENCHMARK_UTIL_HPP
