#include "sample_workload.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tuple>

#include "config_options.h"
#include "utils.h"

struct KVPair {
  std::string key;
  std::string value;

  static bool compare_(const KVPair &a, const KVPair &b) {
    return a.key.compare(b.key) < 0;
  }
};

struct PerformanceMatrix {
  double insertTime;    // average insert time per unit
  double sortingTime;   // Only used in vector: average sorting time of a full vector
  double readTime;      // average Point read time per unit
  double scanTime;      // average Range scan time per unit
  double sstFlushTime;  // Only needed by SkipList and HashSkipList: average sst flush time
  double sstReadTime;   // Only needed by SkipList and HashSkipList: average Point read time per unit in sst files
  double sstScanTime;   // Only needed by SkipList and HashSkipList: average Range scan time per unit in sst files


  static PerformanceMatrix *GetNewPerfMatrix() {
    PerformanceMatrix *matrix = (PerformanceMatrix *)malloc(sizeof(PerformanceMatrix));
    matrix->insertTime = 0;
    matrix->sortingTime = 0;
    matrix->readTime = 0;
    matrix->scanTime = 0;
    matrix->sstFlushTime = 0;
    matrix->sstReadTime = 0;
    matrix->sstScanTime = 0;
    return matrix;
  }

  void PrintPerfMatrix(const char *type) {
    printf("Data Structure Type: %s\n"
           "InsertTime:  %f\n"
           "SortingTime: %f\n"
           "ReadTime:    %f\n"
           "ScanTime:    %f\n"
           "sstFlushTime:%f\n"
           "sstReadTime: %f\n"
           "sstScanTime: %f\n", type,
          insertTime, sortingTime, readTime, scanTime, sstFlushTime, sstReadTime, sstScanTime);
  }
};

std::string gen_random(const int len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
      tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  
  return tmp_s;
}

std::vector<KVPair> GenerateRandomKVPair(int keyLength, int valueLength, int numRecords) {
  std::vector<KVPair> result;
  for(int i = 0; i < numRecords; i++) {
    KVPair kv{gen_random(keyLength), gen_random(valueLength)};
    result.push_back(kv);
  }

  printf("Generating Random KV Pair (keySize = %d, valueSize = %d) finished.\n", keyLength, valueLength);
  return result;
}

/* ERICTODO: KV should already been generated before calling this method */
PerformanceMatrix *TestVectorPerformance(std::vector<KVPair> &kvPairs, Options &options, std::unique_ptr<DBEnv> &env, ReadOptions &read_options,
  WriteOptions &write_options) {
  DB *db;
  std::string vectorDBPath = env->kDBPath + "_vector";
  PerformanceMatrix *perf = PerformanceMatrix::GetNewPerfMatrix();

  // make sure we have the vector memtable representation
  options.memtable_factory.reset(new VectorRepFactory(env->vector_preallocation_size_in_bytes));
  // avoid switching to new memtable data structure since we are running the sample workload
  options.enable_dynamic_index_organization = false;

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(vectorDBPath, options);
    std::cout << "Destroying Vector database ... done" << std::endl;
  }

  // Now open the vector table
  Status s = DB::Open(options, vectorDBPath, &db);
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  Iterator *it = db->NewIterator(read_options);

  // Test 1: test the average insert time here
  // Remember to insert just the right amount of data to make the vector full.
  auto start = std::chrono::high_resolution_clock::now();
  std::string out;
  int i = 0;
  for (i = 0;i < kvPairs.size(); i++) {
    KVPair kv = kvPairs[i];
    // Check if we are about to exceed the memtable size limit here
    db->GetProperty("rocksdb.cur-size-all-mem-tables", &out);
    int curMemTableSize = std::stoi(out);
    // we keep 10 kv pair size to make sure the check here is enough.
    if (curMemTableSize + sizeof(kv) * 10 > env->GetBufferSize()) {
      break;
    }

    s = db->Put(write_options, kv.key, kv.value);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->insertTime = ((double)duration.count() / (double)(i+1));
  printf("Vector: average insert time is %f\n", perf->insertTime);
  // Record what records we have inserted here
  std::vector<KVPair> insertedKV;
  insertedKV.assign(kvPairs.begin(), kvPairs.begin() + i);

  // Test 2: Test the average sorting time (including copy)
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; i++) {
    auto newVector = insertedKV;
    std::sort(newVector.begin(), newVector.end(), KVPair::compare_);
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->sortingTime = ((double)duration.count() / (double)100);
  printf("Vector: average sorting time is %f\n", perf->sortingTime);

  // Test 3: Test the average pointReadTime (after sort)
  auto sortedKV = insertedKV;
  std::sort(sortedKV.begin(), sortedKV.end(), KVPair::compare_);
  start = std::chrono::high_resolution_clock::now();
  for (auto kv : sortedKV) {
    (void)std::equal_range(sortedKV.begin(), sortedKV.end(), kv,
                    KVPair::compare_);
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->readTime = ((double)duration.count() / (double)sortedKV.size());
  printf("Vector: average point searching time is %f\n", perf->sortingTime);

  // Test 4: Test the average rangeScanTime (after sort)
  // Actually, we can simply calculate it as: average pointReadTime + tranversing half number of records
  start = std::chrono::high_resolution_clock::now();
  i = 0;
  for (auto kv : sortedKV) {
    i++;
    if (i > sortedKV.size() / 2) {
      break;
    };
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->scanTime = (double) duration.count() + perf->scanTime;
  printf("Vector: average range searching time is %f\n", perf->readTime);

  delete it;
  s = db->Close();
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  return perf;
}


int runSampleWorkload(std::unique_ptr<DBEnv> &env) {
  // Step 0: Parse options given by the environment
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options,
                &flush_options);

  // Add custom listners
  std::shared_ptr<CompactionsListner> compaction_listener =
    std::make_shared<CompactionsListner>();
  options.listeners.emplace_back(compaction_listener);

  // ERICTODO: Need the flush listener to help us retrieve the flush time data
  // std::shared_ptr<FlushListner> flush_listener =
  //   std::make_shared<FlushListner>(buffer);
  // options.listeners.emplace_back(flush_listener);

  // Step 1: Generate sample workload here
  int keyLength = (int)(env->kv_entry_size * env->key_value_size_ratio);
  int valueLength = env->kv_entry_size - keyLength;
  std::vector<KVPair> kvPairs = GenerateRandomKVPair(keyLength, valueLength, env->num_kv_entries);

  // Step 2: Now, test the vector workload performance here using the sampled workload
  PerformanceMatrix *vectorPerf = TestVectorPerformance(kvPairs, options, env, read_options, write_options);
  vectorPerf->PrintPerfMatrix("Vector");

  delete vectorPerf;
  return 0;
}