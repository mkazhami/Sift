# Sift
## Getting started
### Prerequisites
* RDMA-capable hardware, RDMA drivers (system-dependent)
* cmake

### Installation
Build the following submodules:
* [farmhash](https://github.com/google/farmhash)
* [cm256cc](https://github.com/f4exb/cm256cc) (OPTIONAL - if using erasure codes)
* [rocksdb](https://github.com/facebook/rocksdb) (OPTIONAL - if enabling persistent storage)

To build Sift:
1. `mkdir build`
2. `cd build`
3. `cmake ..`
4. `make`

Sift's configuration is set at compile-time. You can configure both the replicated memory and key-value systems, including various parameters and flags, in `common/common.h`.

### Running Sift
1. Start the memory nodes by running `build/rdma_server PORT` for each node.
2. Start the key-value store server by running `build/kv_remote_server LISTEN_ADDR LISTEN_PORT SERVER_ID`
3. Start the default key-value store client by running `build/kv_remote_client SERVER_ADDR SERVER_PORT NUM_OPS READ_PROB`, where READ_PROB is in the range [0, 100]

To run Sift with erasure codes, perform the same steps in SiftEC/, which is a fork of Sift that uses the cm256cc library.
