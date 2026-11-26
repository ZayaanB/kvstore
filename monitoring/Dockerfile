FROM ubuntu:24.04 AS build
RUN apt-get update && apt-get install --no-install-recommends -y cmake g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY CMakeLists.txt *.cpp *.hpp ./
COPY benchmarks/benchmark_build.hpp.in benchmarks/
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF && \
    cmake --build build --target kvstore_cli -j

FROM ubuntu:24.04
RUN apt-get update && apt-get install --no-install-recommends -y libstdc++6 && \
    rm -rf /var/lib/apt/lists/* && mkdir /data && chown 10001:10001 /data
COPY --from=build /source/build/kvstore_cli /usr/local/bin/kvstore_cli
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/kvstore_cli"]
