# Anatomy of the LSM Memory Buffer: CS848 Project Version

This repository is forked from the original LSMMemoryProfiling repository developed by the SSD lab in Brandeis University (repo link: https://github.com/SSD-Brandeis/LSMMemoryProfiling). For the CS848 Project, I added a random sampled benchmark based on their benchmark framework. Please follow the steps below to compile the benchmark and check section [Run the Sampled Benchmark](#run-the-sampled-benchmark) to run the sample mini-benchmark developed for the CS848 Project.

## Pre-requisites
The step 1 is to clone this respository in your local machine. You can do this by running the following command:
```bash
git clone https://github.com/ericpolo/LSMMemoryProfiling
```

You might also need `cmake` and `make`. Other than this you may need to install `libgflags-dev` for RocksDB. You can install this by running the following command:
```bash
sudo apt-get install libgflags-dev
```

After installing the dependencies, you can run the following command to install all the submodules and build the project. Make sure you are in the root directory of the project:
```bash
git submodule update --init --recursive
mkdir build
cd build
cmake ..
make -j66  # 66 is the number of cores, change if required
```

The above command will do the following:
1. Install the `KV-WorkloadGenerator` submodule.
2. Build the RocksDB source code.
3. Build the `KV-WorkloadGenerator` source code.
4. Build the `working_version` source code. (This you can find in `./MemoryProfiling/examples/__working_branch` directory)

Binaries will be installed in the `./bin` folder.
## Running the benchmarks (Original)

### Step 1: Generating the workload
To run any benchmark, you have to first generate the workload. You can do this by going to the `KV-WorkloadGenerator` directory and running the following command:
```bash
./load_gen -I 100 -U 50 -Q 50 -S 100 -Y 0.1

# -I: Inserts
# -U: Updates
# -Q: Point Queries
# -S: Range Queries
# -Y: Range Query Selectivity

# You can also read the README file of KV-WorkloadGenerator for more details.
```
**Note**: The above command will generate a `workload.txt` file in the same directory.

### Step 2: Running the benchmark
After generating the workload, you may want to copy and paste the `workload.txt` to the `./MemoryProfiling/examples/__working_branch` directory.

Great! Now you can run the benchmark by running the following command:
```bash
cd MemoryProfiling/examples/__working_branch

./working_version 
```

The above command will run the benchmark with default parameters. You can also pass different parameters to the benchmark. For example, if you want to run the benchmark with `vector` write buffer, you can run the following command:
```bash
./working_version --memtable_factory=2
```

### Step 3: Analyzing the results
After running the benchmark, you can find the results in the `./MemoryProfiling/examples/__working_branch` directory. The execution will generate a workload.log file which contains the execution details including the time taken for each operation. You can also find the `db_working_home` directory which contains the RocksDB database files.



---
> The `working_verion` takes a few arguments as input. Here is the list of arguments:
```bash
RocksDB_parser.

  OPTIONS:

      This group is all exclusive:
        -d[d], --destroy=[d]              Destroy and recreate the database
                                          [def: 1]
        --cc=[cc]                         Clear system cache [def: 1]
        -T[T], --size_ratio=[T]           The size ratio for the LSM [def: 10]
        -P[P], --buffer_size_in_pages=[P] The number of pages in memory buffer
                                          [def: 4096]
        -B[B], --entries_per_page=[B]     The number of entries in one page
                                          [def: 4]
        -E[E], --entry_size=[E]           The size of one entry you have in
                                          workload.txt [def: 1024 B]
        -M[M], --memory_size=[M]          The memory buffer size in bytes [def:
                                          16 MB]
        -f[file_to_memtable_size_ratio],
        --file_to_memtable_size_ratio=[file_to_memtable_size_ratio]
                                          The ratio between files and memtable
                                          [def: 1]
        -F[file_size],
        --file_size=[file_size]           The size of one SST file [def: 256 KB]
        -V[verbosity],
        --verbosity=[verbosity]           The verbosity level of execution
                                          [0,1,2; def: 0]
        -c[compaction_pri],
        --compaction_pri=[compaction_pri] [Compaction priority: 1 for
                                          kMinOverlappingRatio, 2 for
                                          kByCompensatedSize, 3 for
                                          kOldestLargestSeqFirst, 4 for
                                          kOldestSmallestSeqFirst; def: 1]
        -C[compaction_style],
        --compaction_style=[compaction_style]
                                          [Compaction priority: 1 for
                                          kCompactionStyleLevel, 2 for
                                          kCompactionStyleUniversal, 3 for
                                          kCompactionStyleFIFO, 4 for
                                          kCompactionStyleNone; def: 1]
        -b[bits_per_key],
        --bits_per_key=[bits_per_key]     The number of bits per key assigned to
                                          Bloom filter [def: 10]
        --bb=[bb]                         Block cache size in MB [def: 8 MB]
        -s[show_progress],
        --sp=[show_progress]              Show progress [def: 0]
        -t[del_per_th],
        --dpth=[del_per_th]               Delete persistence threshold [def: -1]
        --stat=[enable_rocksdb_perf_iostat]
                                          Enable RocksDBs internal Perf and
                                          IOstat [def: 0]
        -i[inserts], --inserts=[inserts]  The number of unique inserts to issue
                                          in the experiment [def: 1]
        -m[memtable_factory],
        --memtable_factory=[memtable_factory]
                                          [Memtable Factory: 1 for Skiplist, 2
                                          for Vector, 3 for Hash Skiplist, 4 for
                                          Hash Linkedlist; def: 1]
        -X[prefix_length],
        --prefix_length=[prefix_length]   [Prefix Length: Number of bytes of the
                                          key forming the prefix; def: 0]
        -H[bucket_count],
        --bucket_count=[bucket_count]     [Bucket Count: Number of buckets for
                                          the hash table in HashSkipList &
                                          HashLinkList Memtables; def: 50000]
        --threshold_use_skiplist=[threshold_use_skiplist]
                                          [Threshold Use SkipList: Threshold
                                          based on which the conversion will
                                          happen from HashLinkList to
                                          HashSkipList; def: 256]
        -A[preallocation_vector_size],
        --preallocation_size=[preallocation_vector_size]
                                          [Preallocation Vector Size: Size to
                                          preallocation to vector memtable; def:
                                          0]
```


## Run the Sampled Benchmark
The sampled benchmark is developed for the CS848 Project to collect basic operation costs of different data structures. It is developed based on the available benchmark framework and shares the same compilation process and executable. It does not require pre-generating the workload, but it generates random KV pairs on the fly. It currently supports three data structures: Vector, SkipList, and HashSkipList. To run the random-sampled mini-benchmark, simply go to the bin folder and run the following command with newly added options:
```bash
# run the sampled benchmark with
#     Entry size 116
#     Ratio of key size for each entry 0.14     <- key size 16, data size 100
#     Hash prefix length 6
#     Max number of random KV pairs 20000
#     The size ratio for the LSM 4
#     Max in-memory component memory usage 67108864 = 64 MB
./working_version -s 1 -e 116 -r 0.14 -X 6 -n 200000 --size_ratio=4 -M 67108864

# New options added to support sampled benchmark:
New options:
        -s[run_sample_workload],
        --sample_workload=[run_sample_workload]
                                          [Run 848 Project sample workload or
                                          not: 0 for No, 1 for Yes; def: 0]
        -e[kv_entry_size],
        --kv_entry_size=[kv_entry_size]   [Entry size in bytes;def: 8]
        -r[key_value_size_ratio],
        --key_value_size_ratio=[key_value_size_ratio]
                                          [Ratio of key size for each entry; def:
                                          0.5]
        -n[num_kv_entries],
        --num_kv_entries=[num_kv_entries] [Number of kv entries will be
                                          generated in the sample workload; def:
                                          20000]
```

The result will be saved in the file ./sample_workload.stat for later reference in RocksDB.
To run RocksDB, you need to define the environment variable *SAMPLE_WORKLOAD_STAT_PATH* to be the path to the sample_workload.stat file in your bash profile:
```bash
export SAMPLE_WORKLOAD_STAT_PATH="~/path/to/sample_workload.stat"
```

### Other information
For the ease of finding information to run the project, here is the summary to run the other components of this project.

RocksDB code is in branch VA_new of github repo: https://github.com/ericpolo/rocksdb_VA . You can also navigate to ./lib/rocksdb which is linked to the aforementioned branch.

To compile RocksDB, use the following command to compile:
```bash
cd rocksdb_VA
make -sj12 release
```
This will also compile dbBench, which is located at the home directory of the RocksDB repository.

To run dbBench, use the following command to run the fillRandom workload:
```bash
# Database directory is ./myData
# Initial memtable representation is skip_list
#   Avaliable options: vector, skip_list, prefix_hash, hash_skiplist
#     To run a hashed data structure, you also need to specify the key prefix length by the option prefix_size
#       i.e. add -prefix_size=8 to specify the key prefix length as 8
# Allow_concurrent_memtable_write to disable concurrent insertion when using skip_list to test raw performance
# Number of entries = 1,000,000
# 
# run ./db_bench -help to see other options
./db_bench --benchmarks="fillrandom" --db=./myData --memtablerep=skip_list --allow_concurrent_memtable_write=false -num 1000000
```

The baseline of RocksDB is in the branch baseline of the same GitHub repo. You can navigate to ./lib/rocksdb and check out baseline branch using the following command:
```
git checkout baseline
```
Compilation of the mini-benchmark will also compile the RocksDB library.


