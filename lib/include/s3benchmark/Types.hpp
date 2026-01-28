//
// Created by maxi on 08.06.20.
//

#ifndef _S3BENCHMARK_TYPES_HPP
#define _S3BENCHMARK_TYPES_HPP

#include <chrono>
#include <ratio>
#include <vector>
#include <string>
#include <sstream>
#include "Util.hpp"

namespace s3benchmark {
    using latency_t = uint64_t;
    struct Latency {
        latency_t first_byte;
        latency_t last_byte;
    };

    struct ByteRange {
        size_t first_byte;
        size_t last_byte;

        inline std::string as_http_header() const {
            std::ostringstream s;
            s << "bytes=" << first_byte << "-" << last_byte;
            return s.str();
        }

        inline size_t length() const {
            return last_byte - first_byte;
        }
    };

    struct RunParameters {
        size_t sample_count;
        size_t thread_count;
        size_t payload_size;
    };

    struct RunResults {
        std::vector<latency_t> data_points;
        latency_t overall_time;
    };

    struct RunStats : public RunParameters {
        double throughput_mbps;
        latency_t latency_avg;
        latency_t latency_sum;
        latency_t latency_min;
        latency_t latency_max;
        latency_t duration;
        size_t download_sum;
        size_t samples_sum;

        RunStats(const RunParameters &params, const RunResults &run)
            : RunParameters(params)
            , samples_sum(run.data_points.size())
            , duration(run.overall_time) {
            latency_t l_min, l_max = run.data_points[0];
            latency_t l_sum = 0;
            for (auto& dp : run.data_points) {
                if (dp < l_min) l_min = dp;
                if (dp > l_max) l_max = dp;
                l_sum += dp;
            }
            latency_t l_avg = l_sum / this->samples_sum;
            this->download_sum = samples_sum * params.payload_size;
            this->throughput_mbps = (download_sum * 1.0 / units::mib) / (duration * 1.0 / units::ms_per_sec);
            this->latency_avg = l_avg;
            this->latency_min = l_min;
            this->latency_max = l_max;
            this->latency_sum = l_sum;
        }
    };
}

#endif  // _S3BENCHMARK_TYPES_HPP
