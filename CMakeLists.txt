# ---------------------------------------------------------------------------
# S3 Benchmark
# ---------------------------------------------------------------------------

project(S3Benchmark LANGUAGES C CXX ASM)
cmake_minimum_required(VERSION 3.10.2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

set(BENCH_LIB_DIR "${CMAKE_SOURCE_DIR}/lib/")
set(BENCH_CLI_DIR "${CMAKE_SOURCE_DIR}/cli/")

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/vendor/googletest.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/cpplint.cmake")
# include("${CMAKE_SOURCE_DIR}/vendor/awssdk.cmake")
include("${CMAKE_SOURCE_DIR}/vendor/gflags.cmake")

set(AWS_REQUIRED_LIBS
  aws-cpp-sdk-s3                 # libaws-cpp-sdk-s3.a
  aws-cpp-sdk-core               # libaws-cpp-sdk-core.a
  aws-cpp-sdk-kms                # libaws-cpp-sdk-kms.a
  aws-cpp-sdk-s3-encryption      # libaws-cpp-sdk-s3-encryption.a
  aws-c-event-stream             # libaws-c-event-stream.a
  aws-checksums                  # libaws-checksums.a
  aws-c-io
  aws-c-common                   # libaws-c-common.a
  )
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)
find_package(ZLIB REQUIRED)
 set(AWS_LINK_LIBRARIES
	     aws-cpp-sdk-s3
    aws-cpp-sdk-core
    aws-c-event-stream
    aws-c-auth
    aws-c-io
    aws-checksums
    aws-c-common
    s2n
    OpenSSL::Crypto
    CURL::libcurl
    z
    pthread
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

include("${BENCH_LIB_DIR}/local.cmake")
include("${BENCH_CLI_DIR}/local.cmake")


include("${CMAKE_SOURCE_DIR}/test/local.cmake")

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_custom_target(lint)
add_dependencies(lint ${lint_targets})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

message(STATUS "[S3 BENCHMARK] settings")
message(STATUS "    BENCH_LIB_DIR               = ${BENCH_LIB_DIR}")
message(STATUS "    BENCH_CLI_DIR               = ${BENCH_CLI_DIR}")
message(STATUS "[CMAKE] settings")
message(STATUS "    CMAKE_SOURCE_DIR            = ${CMAKE_SOURCE_DIR}")
message(STATUS "    CMAKE_SHARED_LIBRARY_SUFFIX = ${CMAKE_SHARED_LIBRARY_SUFFIX}")
message(STATUS "    CMAKE_STATIC_LIBRARY_SUFFIX = ${CMAKE_STATIC_LIBRARY_SUFFIX}")
message(STATUS "    CMAKE_SHARED_LIBRARY_PREFIX = ${CMAKE_SHARED_LIBRARY_PREFIX}")
message(STATUS "    CMAKE_STATIC_LIBRARY_PREFIX = ${CMAKE_STATIC_LIBRARY_PREFIX}")
message(STATUS "[AWSSDK] settings")
message(STATUS "    AWS_INCLUDE_DIR             = ${AWS_INCLUDE_DIR}")
message(STATUS "    AWS_LINK_LIBRARIES          = ${AWS_LINK_LIBRARIES}")
message(STATUS "[CPPLINT] settings")
message(STATUS "    CPPLINT_SCRIPT_PATH         = ${CPPLINT_SCRIPT_PATH}")
message(STATUS "    lint_targets                = ${CMAKE_CXX_CPPLINT}")
message(STATUS "[TEST] settings")
message(STATUS "    GTEST_INCLUDE_DIR           = ${GTEST_INCLUDE_DIR}")
message(STATUS "    GTEST_LIBRARY_PATH          = ${GTEST_LIBRARY_PATH}")
message(STATUS "    GMOCK_INCLUDE_DIR           = ${GMOCK_INCLUDE_DIR}")
message(STATUS "    GMOCK_LIBRARY_PATH          = ${GMOCK_LIBRARY_PATH}")
