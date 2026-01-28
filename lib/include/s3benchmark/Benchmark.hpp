//
// Created by Maximilian Kuschewski on 2020-05-06
//

#ifndef _S3BENCHMARK_BENCHMARK_HPP
#define _S3BENCHMARK_BENCHMARK_HPP

#include <ratio>
#include <span>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include "Config.hpp"
#include "Logger.hpp"

namespace s3benchmark {
    using ObjectHead = Aws::S3::Model::HeadObjectOutcome;
    using S3Op = std::function<void(Aws::S3::S3Client&)>;

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
        [[nodiscard]] RunResults do_run(RunParameters &params);
        void run_full_benchmark(Logger &logger);

    private:
        static std::shared_ptr<Aws::IOStream> make_body(size_t length)
        {
            auto body = Aws::MakeShared<Aws::StringStream>("PutObjectBody");
            body->seekp(length - 1);
            *body << '\0';
            body->seekg(0);
            return body;
        }
    };
}

#endif // _S3BENCHMARK_BENCHMARK_HPP
