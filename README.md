
## Example Run
Example run on a c5n.18xlarge instance:
```
./s3benchmark  --threads-min 108 --threads-max 216 --object-name "benchmark/largefile.bin" --samples $((144*3)) --payloads-min $((1024*1024*6)) --payloads-max $((1024*1024*24))
```

## Record performance profile with callstack
```
sudo perf record --freq=997 --call-graph dwarf -q ./s3benchmark  --threads-min 72 --threads-max 72 --object-name "benchmark/largefile.bin" --samples 20 --payloads-min $((1024*1024*12)) --payloads-max $((1024*1024*12))
```

## Dependencies

`sudo apt-get install zlib1g-dev libssl-dev cmake g++ libcurl4-openssl-dev`

It is **MUCH** easier to install `aws-sdk-cpp` library independently:
```shell
git clone git@github.com:aws/aws-sdk-cpp.git
cd aws-sdk-cpp
mkdir release && cd release
cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="s3"
make -j
sudo make install
```

## Compilation

```shell
cmake -DBENCH_TARGET=cli ..
make -j
```
