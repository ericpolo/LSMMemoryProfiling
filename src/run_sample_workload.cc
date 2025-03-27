#include "sample_workload.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tuple>
#include <thread>

#include "config_options.h"
#include "utils.h"

std::string sample_buffer_file = std::getenv("SAMPLE_WORKLOAD_STAT_PATH");
const int MAX_RESERVED_ENTRY_COUNT = 10;

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

  double numEntriesRatioToVec; // ratio of number of entries current data structure can hold when the memtable is full/scheduled
                               // for flush compared to Vector since Vector has the lowest memory overhead

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
    printf("Data Structure Type:  %s\n"
           "InsertTime:           %f\n"
           "SortingTime:          %f\n"
           "ReadTime:             %f\n"
           "ScanTime:             %f\n"
           "sstFlushTime:         %f\n"
           "sstReadTime:          %f\n"
           "sstScanTime:          %f\n"
           "numEntriesRatioToVec: %f\n", type,
          insertTime, sortingTime, readTime, scanTime, sstFlushTime, sstReadTime, sstScanTime, numEntriesRatioToVec);
  }

  void FlushToBuffer(std::shared_ptr<Buffer> buffer) {
    (*buffer) << insertTime << std::endl
              << sortingTime << std::endl
              << readTime << std::endl
              << scanTime << std::endl
              << sstFlushTime << std::endl
              << sstReadTime << std::endl
              << sstScanTime << std::endl
              << numEntriesRatioToVec << std::endl;
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

PerformanceMatrix *TestVectorPerformance(std::vector<KVPair> &kvPairs, Options &options, std::unique_ptr<DBEnv> &env, ReadOptions &read_options,
  WriteOptions &write_options, int &numEntries) {
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

  // Test 1: test the average insert time here
  // Remember to insert just the right amount of data to make the memtable full.
  unsigned long insertTimeTotal = 0;
  int i = 0;
  size_t reservedSpace = sizeof(KVPair) * MAX_RESERVED_ENTRY_COUNT;
  for (i = 0;i < kvPairs.size(); i++) {
    KVPair kv = kvPairs[i];
    // Check if we the memtable is about to be scheduled to flush before current insert
    if (db->ShouldMemTableFlushNow(reservedSpace)) {
      break;
    }

    auto start = std::chrono::high_resolution_clock::now();
    s = db->Put(write_options, kv.key, kv.value);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    insertTimeTotal += duration.count();
  }
  perf->insertTime = ((double)insertTimeTotal / (double)(i));
  printf("Vector: average insert time is %f for %d inserts\n", perf->insertTime, i);
  // Record what records we have inserted here
  std::vector<KVPair> insertedKV;
  insertedKV.assign(kvPairs.begin(), kvPairs.begin() + i);

  // Record number of entries we can hold in a full Vector for future reference
  numEntries = i + MAX_RESERVED_ENTRY_COUNT;
  perf->numEntriesRatioToVec = 1;
  printf("Vector: Number Entries a full vector can hold is around %d\n", numEntries);

  // Test 2: Test the average sorting time (including copy)
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; i++) {
    auto newVector = insertedKV;
    std::sort(newVector.begin(), newVector.end(), KVPair::compare_);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->sortingTime = ((double)duration.count() / (double)100);
  printf("Vector: average sorting time is %f\n", perf->sortingTime);

  // Test 3: Test the average pointReadTime (after sort)
  auto sortedInsertedKV = insertedKV;
  std::sort(sortedInsertedKV.begin(), sortedInsertedKV.end(), KVPair::compare_);
  start = std::chrono::high_resolution_clock::now();
  for (auto kv : sortedInsertedKV) {
    (void)std::equal_range(sortedInsertedKV.begin(), sortedInsertedKV.end(), kv,
                    KVPair::compare_);
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->readTime = ((double)duration.count() / (double)sortedInsertedKV.size());
  printf("Vector: average point searching time is %f\n", perf->readTime);

  // Test 4: Test the average rangeScanTime (after sort)
  // Actually, we can simply calculate it as: average pointReadTime + tranversing half number of records
  start = std::chrono::high_resolution_clock::now();
  i = 0;
  for (auto kv : sortedInsertedKV) {
    i++;
    if (i > sortedInsertedKV.size() / 2) {
      break;
    };
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->scanTime = (double) duration.count() + perf->scanTime;
  printf("Vector: average range searching time is %f\n", perf->readTime);

  s = db->Close();
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  return perf;
}

PerformanceMatrix *TestSkipListPerformance(std::vector<KVPair> &kvPairs, Options &options, std::unique_ptr<DBEnv> &env, ReadOptions &read_options,
  WriteOptions &write_options, const int numEntriesVec, bool isHashed = false) {
  DB *db;
  std::string dbPath = env->kDBPath;
  std::string memTableType;
  if (isHashed) {
    dbPath += "_hashed_skiplist";
    // make sure we have the vector memtable representation
    options.memtable_factory.reset(
      NewHashSkipListRepFactory(env->bucket_count, env->skiplist_height,
                                env->skiplist_branching_factor));
    options.prefix_extractor.reset(
      NewFixedPrefixTransform(env->prefix_length));
      memTableType = "HashSkipList";
    if (env->prefix_length == 0) {
      printf("Error: prefix_length is 0. Need to specify the prefix length before testing the performance for HashSkipList\n");
      return nullptr;
    }
  } else {
    dbPath += "_skiplist";
    // make sure we have the vector memtable representation
    options.memtable_factory.reset(new SkipListFactory);
    memTableType = "SkipList";
  }
  PerformanceMatrix *perf = PerformanceMatrix::GetNewPerfMatrix();

  // avoid switching to new memtable data structure since we are running the sample workload
  options.enable_dynamic_index_organization = false;

  // Create a flush listner to test flush time
  std::shared_ptr<FlushListner> flush_listener = std::make_shared<FlushListner>();
  options.listeners.emplace_back(flush_listener);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(dbPath, options);
    std::cout << "Destroying SkipList database ... done" << std::endl;
  }

  // Now open the vector table
  Status s = DB::Open(options, dbPath, &db);
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  Iterator *it = db->NewIterator(read_options);

  // Test 1: test the average insert time here
  // Remember to insert just the right amount of data to make the vector full.
  unsigned long insertTimeTotal = 0;
  int i = 0;
  size_t reservedSpace = sizeof(KVPair) * MAX_RESERVED_ENTRY_COUNT;
  for (i = 0;i < kvPairs.size(); i++) {
    KVPair kv = kvPairs[i];
    // Check if we the memtable is about to be scheduled to flush before current insert
    if (db->ShouldMemTableFlushNow(reservedSpace)) {
      break;
    }

    auto start = std::chrono::high_resolution_clock::now();
    s = db->Put(write_options, kv.key, kv.value);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
    insertTimeTotal += duration.count();
  }
  perf->insertTime = ((double)insertTimeTotal / (double)(i));
  printf("%s: average insert time is %f for %d inserts\n", memTableType.c_str(), perf->insertTime, i);
  // Record what records we have inserted here
  std::vector<KVPair> insertedKV;
  insertedKV.assign(kvPairs.begin(), kvPairs.begin() + i);

  // Record the ratio of num entreis the current data structure can hold at most compared to Vector
  int numEntries = i + MAX_RESERVED_ENTRY_COUNT;
  perf->numEntriesRatioToVec = (double)((double)(numEntries) / double(numEntriesVec));
  printf("%s: Number Entries at most can hold is around %d, ratio to vector %f\n", memTableType.c_str(), numEntries, perf->numEntriesRatioToVec);

  // Test 2: Test the average reading time (random)
  // We test (insertedKV.size() / 10) reads here and get the average
  auto start = std::chrono::high_resolution_clock::now();
  int maxNumRead = insertedKV.size() / 10;
  for (int i = 0; i < maxNumRead; i++) {
    int index = rand() % insertedKV.size();
    KVPair kv = insertedKV[index];
    std::string value;

    s = db->Get(read_options, kv.key, &value);
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->readTime = ((double)duration.count() / (double)maxNumRead);
  printf("%s: average reading time is %f\n", memTableType.c_str(), perf->readTime);

  // Test 3: Test the average range scan time of the memtable
  std::string start_key, end_key;
  auto sortedInsertedKV = insertedKV;
  std::sort(sortedInsertedKV.begin(), sortedInsertedKV.end(), KVPair::compare_);
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; i++) {
    int start_i = rand() % sortedInsertedKV.size();
    int end_i = start_i + rand() %(sortedInsertedKV.size() - start_i);
    start_key = sortedInsertedKV[start_i].key;
    end_key = sortedInsertedKV[end_i].key;
    it->Refresh();
    assert(it->status().ok());
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      if (it->key().ToString() >= end_key) {
        break;
      }
    }
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->scanTime = ((double)duration.count() / (double)100);
  printf("%s: average range search time is %f\n", memTableType.c_str(), perf->scanTime);

  // Test 4: We need to test the SSTFlush time here
  //     Now, Insert new keys until the flush starts
  for (int i = sortedInsertedKV.size(); i < kvPairs.size(); i++) {
    s = db->Put(write_options, kvPairs[i].key, kvPairs[i].value);
  }

  int numFlush = flush_listener->GetNumFlush();
  if (numFlush == 0) {
    printf("%s: NO flush happened, PLEASE increase the number of randomly sampled records\n", memTableType.c_str());
    return perf;
  }
  auto flushDurations = flush_listener->GetFlushDurations();
  int64_t totalDuration = 0;
  i = 1;
  for (auto duration : flushDurations) {
    // printf("For %dth flush, duration is %lld\n", i, duration);
    totalDuration += duration;
    i++;
  }
  perf->sstFlushTime = totalDuration / numFlush;
  printf("%s: average SST flush time is %f\n", memTableType.c_str(), perf->sstFlushTime);

  // Test 5: Test the SST Point Query Time WITHIN 1 SST file
  //    We know for sure that any KV Pair in insertedKV must be flushed to disk.
  int maxReadCount = 1000;
  std::string value;
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < maxReadCount; i++) {
    int index = rand() % insertedKV.size();
    s = db->Get(read_options, insertedKV[index].key, &value);
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->sstReadTime = ((double)duration.count() / (double)maxReadCount);
  printf("%s: average SST point scan time is %f\n", memTableType.c_str(), perf->sstReadTime);

  // Test 6: Test the SST Range Query Time
  auto sortedKV = kvPairs;
  std::sort(sortedKV.begin(), sortedKV.end(), KVPair::compare_);
  int scanKeyNum = (int)((float)sortedInsertedKV.size() * env->range_query_selectivity);
  int startLimit = sortedKV.size() - scanKeyNum;
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 100; i++) {
    int start_i = rand() % startLimit;
    int end_i = start_i + scanKeyNum;
    assert(end_i <= sortedKV.size());
    start_key = sortedKV[start_i].key;
    end_key = sortedKV[end_i].key;
    it->Refresh();
    assert(it->status().ok());
    for (it->Seek(start_key); it->Valid(); it->Next()) {
      if (it->key().ToString() >= end_key) {
        break;
      }
    }
  }
  stop = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  perf->sstScanTime = ((double)duration.count() / (double)100);
  printf("%s: average SST range search time is %f\n", memTableType.c_str(), perf->sstScanTime);

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
  std::shared_ptr<Buffer> buffer = std::make_unique<Buffer>(sample_buffer_file);

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
  int numEntries = 0;
  PerformanceMatrix *vectorPerf = TestVectorPerformance(kvPairs, options, env, read_options, write_options, numEntries);
  // vectorPerf->PrintPerfMatrix("Vector");

  // Step 3: Test the skiplist workload performance here using the sampled workload
  PerformanceMatrix *skipListPerf = TestSkipListPerformance(kvPairs, options, env, read_options, write_options, numEntries);
  // skipListPerf->PrintPerfMatrix("SkipList");

  // Step 3: Test the hashskiplist workload performance here using the sampled workload
  PerformanceMatrix *hashSkipListPerf = TestSkipListPerformance(kvPairs, options, env, read_options, write_options, numEntries, true /* ishashed */);
  // hashSkipListPerf->PrintPerfMatrix("HashSkipList");

  skipListPerf->FlushToBuffer(buffer);
  vectorPerf->FlushToBuffer(buffer);
  hashSkipListPerf->FlushToBuffer(buffer);
  buffer->flush();

  printf("Statistics of the random sample workload is flushed to file: %s\n", sample_buffer_file.c_str());
  delete vectorPerf;
  delete skipListPerf;
  delete hashSkipListPerf;
  return 0;
}