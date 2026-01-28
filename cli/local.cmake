# ---------------------------------------------------------------------------
# S3 Benchmark - CLI tool
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Include Project Directories
# ---------------------------------------------------------------------------
include("${BENCH_CLI_DIR}/include/local.cmake")
include_directories(
    ${BENCH_CLI_DIR}/include
    ${GFLAGS_INCLUDE_DIR}
    ${GTEST_INCLUDE_DIR}
    ${GMOCK_INCLUDE_DIR}
    ${GFLAGS_INCLUDE_DIR}
)

include("${BENCH_CLI_DIR}/src/local.cmake")

add_executable(s3benchmark_cli_exec ${BENCH_CLI_SRC_CC})
add_dependencies(s3benchmark_cli_exec s3benchmark_lib)
target_link_libraries(s3benchmark_cli_exec s3benchmark_lib ${GFLAGS_LINK_LIBRARIES} ${AWS_LINK_LIBRARIES})
