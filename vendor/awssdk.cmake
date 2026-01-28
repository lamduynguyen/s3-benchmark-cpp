# ---------------------------------------------------------------------------
# S3 Benchmark - AWS SDK
# ---------------------------------------------------------------------------
Include(ExternalProject)

# Because we are building static libraries,
# all sub-dependencies of the libraries we
# actually want have to be included
# ORDER MATTERS for the linker
set(AWS_REQUIRED_LIBS
  aws-cpp-sdk-s3                 # libaws-cpp-sdk-s3.a
  aws-cpp-sdk-core               # libaws-cpp-sdk-core.a
  aws-cpp-sdk-kms                # libaws-cpp-sdk-kms.a
  aws-cpp-sdk-s3-encryption      # libaws-cpp-sdk-s3-encryption.a
  aws-c-event-stream             # libaws-c-event-stream.a
  aws-checksums                  # libaws-checksums.a
  aws-c-common                   # libaws-c-common.a
  )

# set(AWS_REQUIRED_LIBS
# aws-c-common        # libaws-c-common.a
# aws-c-event-stream  # libaws-c-event-stream.a
# aws-checksums       # libaws-checksums.a
# aws-cpp-sdk-core    # libaws-cpp-sdk-core.a
# aws-cpp-sdk-s3      # libaws-cpp-sdk-s3.a
# )

set(AWS_INSTALL_DIR "vendor/awssdk/install")
set(AWS_BUILD_BYPRODUCTS "")
foreach(LIBNAME ${AWS_REQUIRED_LIBS})
  list(APPEND AWS_BUILD_BYPRODUCTS ${AWS_INSTALL_DIR}/lib/lib${LIBNAME}.a)
endforeach()
message(STATUS "[AWS] AWS_BUILD_BYPRODUCTS = ${AWS_BUILD_BYPRODUCTS}")

ExternalProject_Add(awssdk
  GIT_REPOSITORY      https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG             1.11.736
  PREFIX              "vendor/awssdk"
  INSTALL_DIR         ${AWS_INSTALL_DIR}
  CMAKE_ARGS
    -DBUILD_ONLY=core\\$<SEMICOLON>s3\\$<SEMICOLON>s3-encryption
    -DBUILD_SHARED_LIBS:BOOL=OFF
    -DBUILD_STATIC_LIBS:BOOL=ON
    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    # -DCMAKE_SOURCE_DIR:PATH=<SOURCE_DIR>
    -DENABLE_TESTING:BOOL=OFF
    -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/${AWS_INSTALL_DIR}
    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  BUILD_BYPRODUCTS ${AWS_BUILD_BYPRODUCTS}
)

ExternalProject_Get_Property(awssdk INSTALL_DIR)
set(AWS_INCLUDE_DIR ${INSTALL_DIR}/include)
file(MAKE_DIRECTORY ${AWS_INCLUDE_DIR})
message(STATUS "[AWS] AWS_INSTALL_DIR = ${INSTALL_DIR}")
message(STATUS "[AWS] AWS_INCLUDE_DIR = ${AWS_INCLUDE_DIR}")


foreach(LIBNAME ${AWS_REQUIRED_LIBS})
  add_library(${LIBNAME} STATIC IMPORTED)
  set_property(TARGET ${LIBNAME} PROPERTY IMPORTED_LOCATION ${INSTALL_DIR}/lib/lib${LIBNAME}.a)
  set_property(TARGET ${LIBNAME} APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${AWS_INCLUDE_DIR})
  add_dependencies(${LIBNAME} awssdk)
endforeach()

# Load OpenSSL as a static lib
# set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)

set(AWS_LINK_LIBRARIES ${AWS_REQUIRED_LIBS} OpenSSL::Crypto ${CURL_LIBRARY})
message(STATUS "[AWS] AWS_LINK_LIBRARIES = ${AWS_LINK_LIBRARIES}")
