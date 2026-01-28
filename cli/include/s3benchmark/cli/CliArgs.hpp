//
// Created by Maximilian Kuschewski on 2020-05-06
//
#ifndef _S3BENCHMARK_CLIARGS_HPP
#define _S3BENCHMARK_CLIARGS_HPP

#include <thread>
#include <exception>

#include <gflags/gflags.h>
#include "s3benchmark/Config.hpp"

namespace s3benchmark::cli {

        #pragma clang diagnostic push
        #pragma ide diagnostic ignored "cert-err58-cpp"
        // Define program arguments using gflags macros
        namespace flags {
            DEFINE_bool(bmread, false,
                        "If true, benchmarking object fetch. False means benchmarking object write/put.");
            DEFINE_bool(quiet, false, "If true, log run results etc. to the cli.");
            DEFINE_bool(dry_run, false, "If true, don't do any requests but do a dry run instead");
            DEFINE_bool(threads_static, false,
            "If true, interprete threads-min and threads-max as static counts instead of multiples of the hardware thread count.\n"
            "It's advised to explicitly set threads-min and threads-max if this option is given.");
            DEFINE_double(threads_min, 16, // 1
            "The minimum number of threads to use when fetching objects from S3 as a multiple of the hardware thread count.");
            DEFINE_double(threads_max, 32, // 2
            "The maximum number of threads to use when fetching objects from S3 as a multiple of the hardware thread count.");
            DEFINE_double(threads_step, 2,
            "What increase in thread count per benchmark run is. Positive means multiplicative, negative means additive.");
            DEFINE_uint64(payloads_min, 128,
            "The minimum object size to test (in bytes), and every increment is a double of the previous value.");
            DEFINE_uint64(payloads_max, 4 * units::kib,
            "The maximum object size to test (in bytes), and every increment is a double of the previous value.");
            DEFINE_uint64(payloads_step, 2,
            "What the multiplicative increase in payload size per benchmark run is (size *= step). Must be > 1");
            DEFINE_bool(payloads_reverse, false,
            "If true, start with the largest payload size first and decrease from there");
            DEFINE_uint64(samples, 5,
            "The number of samples to collect for each test of a single object size per thread.");
            DEFINE_uint64(samples_cap, 7200,
            "The maximum number of samples to collect for each test of a single object size.");
            DEFINE_string(bucket_name, Config::DEFAULT_BUCKET_NAME,
            "The name of the bucket where the test object is located");
            DEFINE_string(object_name, Config::DEFAULT_OBJECT_NAME,
            "The name of the large object file where data will be fetched from");
            DEFINE_string(region, Config::DEFAULT_REGION,
            "Sets the AWS region to use for the S3 bucket. Only applies if the bucket doesn't already exist.");
            DEFINE_bool(full, false,
            "Runs the full exhaustive test, and overrides the threads and payload arguments.");
            DEFINE_bool(throttling_mode, false,
            "Runs a continuous test to find out when EC2 network throttling kicks in.");
            DEFINE_string(upload_csv, "",
            "Uploads the test results to S3 as a CSV file.");
            DEFINE_string(upload_stats, "",
            "Upload CPU stats from during the benchmark to S3 as a CSV file.");

        } // namespace flags

        Config config_from_flags(int *argc, char ***argv) {
            gflags::ParseCommandLineFlags(argc, argv, true);
            if (*argc > 1) {
                throw std::runtime_error("More arguments given than necessary. Quitting.");
            }
            return Config({
                    flags::FLAGS_bmread,
                    flags::FLAGS_dry_run,
                    flags::FLAGS_quiet,
                    flags::FLAGS_threads_static,
                    flags::FLAGS_threads_min,
                    flags::FLAGS_threads_max,
                    flags::FLAGS_threads_step,
                    flags::FLAGS_payloads_min,
                    flags::FLAGS_payloads_max,
                    flags::FLAGS_payloads_step,
                    flags::FLAGS_payloads_reverse,
                    flags::FLAGS_samples,
                    flags::FLAGS_samples_cap,
                    flags::FLAGS_bucket_name,
                    flags::FLAGS_object_name,
                    flags::FLAGS_region,
                    flags::FLAGS_full,
                    flags::FLAGS_throttling_mode,
                    flags::FLAGS_upload_csv,
                    flags::FLAGS_upload_stats,
            });
        }

    }  // namespace s3benchmark

#endif // _S3BENCHMARK_CLIARGS_HPP


#pragma clang diagnostic pop