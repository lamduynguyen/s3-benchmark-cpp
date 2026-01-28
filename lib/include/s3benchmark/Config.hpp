//
// Created by Maximilian Kuschewski on 07.06.20.
//
#ifndef _S3BENCHMARK_CONFIG_HPP
#define _S3BENCHMARK_CONFIG_HPP

#include "Util.hpp"
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>

namespace s3benchmark {
    using config_t = Aws::Client::ClientConfiguration;

    struct ConfigParameters {
        bool dry_run;
        bool quiet;
        bool threads_static;
        double threads_min;
        double threads_max;
        double threads_step;
        size_t payloads_min;
        size_t payloads_max;
        size_t payloads_step;
        bool payloads_reverse;
        size_t samples;
        size_t samples_cap;
        std::string bucket_name;
        std::string object_name;
        std::string region;
        bool full;
        bool throttling_mode;
        std::string upload_csv;
        std::string upload_stats;
    };

    struct EC2Config {
        std::string ec2_instance_id;
        std::string ec2_instance_type;
        std::string ec2_region;
        size_t hw_thread_count;
        EC2Config();
    };

    class Config : public ConfigParameters, public EC2Config {
        Aws::Client::ClientConfiguration client_config;

        void sanitize_params();
        void sanitize_client_config();

    public:
        inline const static char* DEFAULT_REGION = "eu-north-1";
        inline const static char* DEFAULT_BUCKET_NAME = "tumi13-testbucket";
        inline const static char* DEFAULT_OBJECT_NAME = "object.bin";

        explicit Config(ConfigParameters &&config);

        const Aws::Client::ClientConfiguration& aws_config() const;
        Aws::String region_name() const;
    };
}

#endif //_S3BENCHMARK_CONFIG_HPP
