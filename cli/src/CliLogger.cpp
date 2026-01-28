//
// Created by maxi on 08.06.20.
//

#include "s3benchmark/cli/CliLogger.hpp"
#include "s3benchmark/Util.hpp"

namespace s3benchmark {

    CliLogger::CliLogger(std::ostream &output) : out(output) {}

    void CliLogger::print_run_header() const {
        out << "                                                      +-----------------------------+--------------------+" << std::endl;
        out << "                                                      |     Completion Time [ms]    |    Sample Count    |" << std::endl;
        out << "+---------+----------------+-----------+--------------+-----------------------------+--------------------+" << std::endl;
        out << "| Threads |     Throughput | Exec Time | Download Sum |  min   max   avg   overall  |  thread   overall  |" << std::endl;
        out << "+---------+----------------+-----------+--------------+-----------------------------+--------------------+" << std::endl;
    }

    void CliLogger::print_run_footer() const {
        out << "+---------+----------------+-----------+--------------+-----------------------------+--------------------+" << std::endl;
    }

    void CliLogger::print_run_stats(const RunStats &stats) const {
        out << format::string_format("| %7d ", stats.thread_count)
            << format::string_format("| \033[1;31m%8.2f MiB/s\033[0m ", stats.throughput_mbps)
            << format::string_format("|\033[1m%8.2f s\033[0m ", stats.duration * 1.0 / (units::ms_per_sec * 1000000UL))
            << format::string_format("|\033[1m%9.0f MiB\033[0m ", stats.download_sum * 1.0 / units::mib)
            << format::string_format("|%5dns %5dns %5dns %9dns  ", stats.latency_min, stats.latency_max, stats.latency_avg, stats.latency_sum)
            << format::string_format("|%8d %9d  |", stats.sample_count, stats.samples_sum)
            << std::endl;
    }

    void CliLogger::print_run_params(const RunParameters &params) const {
        auto bytes = format::byte_format(params.payload_size * 1.0);
        out << "\nDownload performance with \033[1;33m" <<  bytes << "\033[0m objects\n" << std::endl;
    }


    void CliLogger::print_config_params(const ConfigParameters &config) const {
        out << "\n+------------------- \033[1;32mRUN CONFIGURATION\033[0m -------------------+\n";
        auto dry_run = config.dry_run ? "Yes" : "No";
        auto payload_dir = config.payloads_reverse ? "Backward" : "Forward";

        print_conf_var("Read-only Benchmark", config.fetch_benchmark);
        print_conf_var("Dry Run Only", dry_run);
        print_conf_var("EC2 Region", config.region);
        print_conf_var("Bucket Name", config.bucket_name);
        print_conf_var("Object Name", config.object_name);
        print_conf_var("Payloads Min", config.payloads_min);
        print_conf_var("Payloads Max", config.payloads_max);
        print_conf_var("Payloads Step", config.payloads_step);
        print_conf_var("Payloads Dir", payload_dir);
        print_conf_var("Threads Min", config.threads_min);
        print_conf_var("Threads Max", config.threads_max);
        print_conf_var("Threads Step", config.threads_step);
        print_conf_var("Sample Count", config.samples);
        out << "+---------------------------------------------------------+\n\n";

    }

    void CliLogger::print_ec2_config(const EC2Config &config) const {
        out << "\n+------------------- \033[1;32mDETECTED HARDWARE\033[0m -------------------+\n";
        print_conf_var("HW Threads", config.hw_thread_count);
        print_conf_var("Instance Id", config.ec2_instance_id);
        print_conf_var("Instance Type", config.ec2_instance_type);
        print_conf_var("AWS Region", config.ec2_region);
        out << "+---------------------------------------------------------+\n";
    }

}  // namespace s3benchmark::cli
