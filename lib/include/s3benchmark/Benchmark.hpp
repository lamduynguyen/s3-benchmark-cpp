//
// Created by Maximilian Kuschewski on 2020-05-06
//

#ifndef _S3BENCHMARK_BENCHMARK_HPP
#define _S3BENCHMARK_BENCHMARK_HPP

#include <chrono>
#include <ratio>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include "Config.hpp"
#include "Logger.hpp"

namespace s3benchmark {
    using ObjectHead = Aws::S3::Model::HeadObjectOutcome;

    class Benchmark {
        const Config &config;
        Aws::S3::S3Client client;

    public:
        explicit Benchmark(const Config &config);

        void list_buckets() const;
        [[nodiscard]] size_t fetch_object_size() const;
        [[nodiscard]] latency_t fetch_range(const ByteRange &range, char* outbuf, size_t bufsize) const;
        [[nodiscard]] latency_t fetch_object(const Aws::S3::Model::GetObjectRequest &req) const;

        [[nodiscard]] static ByteRange random_range_in(size_t size, size_t max_value) ;
        [[nodiscard]] RunResults do_run(RunParameters &params) const;
        void run_full_benchmark(Logger &logger) const;
    };
}

#endif // _S3BENCHMARK_BENCHMARK_HPP
