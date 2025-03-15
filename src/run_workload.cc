#include "run_workload.h"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <tuple>

#include "config_options.h"
#include "utils.h"

std::string buffer_file = "workload.log";
std::string stats_file = "stats.log";

int runWorkload(std::unique_ptr<DBEnv> &env) {
  DB *db;
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;

  configOptions(env, &options, &table_options, &write_options, &read_options,
                &flush_options);

  std::shared_ptr<Buffer> buffer = std::make_unique<Buffer>(buffer_file);
  std::unique_ptr<Buffer> stats = std::make_unique<Buffer>(stats_file);

  // Add custom listners
  std::shared_ptr<CompactionsListner> compaction_listener =
      std::make_shared<CompactionsListner>();
  options.listeners.emplace_back(compaction_listener);

  std::shared_ptr<FlushListner> flush_listener =
      std::make_shared<FlushListner>(buffer);
  options.listeners.emplace_back(flush_listener);

  if (env->IsDestroyDatabaseEnabled()) {
    DestroyDB(env->kDBPath, options);
    std::cout << "Destroying database ... done" << std::endl;
  }

  PrintExperimentalSetup(env, buffer);

  Status s = DB::Open(options, env->kDBPath, &db);
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  Iterator *it = db->NewIterator(read_options);

#ifdef DOSTO
  if (env->debugging) {
    tree->SetDebugMode(env->debugging);
    tree->PrintFluidLSM(db);
  }
#endif // DOSTO

  // Clearing the system cache
  if (env->clear_system_cache) {
#ifdef __linux__
    std::cout << "Clearing system cache ...";
    std::cout << system("sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'")
              << "done" << std::endl;
#endif
  }

  std::ifstream workload_file;
  workload_file.open("workload.txt");
  assert(workload_file);

  size_t total_operations = 0;
  if (env->IsShowProgressEnabled()) {
    std::string line;
    while (std::getline(workload_file, line)) {
      ++total_operations;
    }
  }

  workload_file.clear();
  workload_file.seekg(0, std::ios::beg);

#ifdef TIMER
  unsigned long inserts_exec_time = 0, updates_exec_time = 0, pq_exec_time = 0,
                pdelete_exec_time = 0, rq_exec_time = 0;
#endif // TIMER
  auto exec_start = std::chrono::high_resolution_clock::now();

  std::string line;
  unsigned long ith_op = 0;
  while (std::getline(workload_file, line)) {
    if (line.empty())
      break;
    bool is_last_line = (workload_file.peek() == EOF);

    std::istringstream stream(line);
    char operation;
    stream >> operation;

    switch (operation) {
      // [Insert]
    case 'I': {
      std::string key, value;
      stream >> key >> value;

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      s = db->Put(write_options, key, value);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "InsertTime: " << duration.count() << std::endl;
      inserts_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [Update]
    case 'U': {
      std::string key, value;
      stream >> key >> value;

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      s = db->Put(write_options, key, value);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "UpdateTime: " << duration.count() << std::endl;
      updates_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [PointDelete]
    case 'D': {
      std::string key;
      stream >> key;

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      s = db->Delete(write_options, key);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "DeleteTime: " << duration.count() << std::endl;
      pdelete_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [ProbePointQuery]
    case 'Q': {
      std::string key, value;
      stream >> key;

#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER
      s = db->Get(read_options, key, &value);
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "GetTime: " << duration.count() << std::endl;
      pq_exec_time += duration.count();
#endif // TIMER
      break;
    }
      // [ScanRangeQuery]
    case 'S': {
      std::string start_key, end_key;
      stream >> start_key >> end_key;

      uint64_t keys_returned = 0, keys_read = 0;
      bool did_run_RR = false;
#ifdef TIMER
      auto start = std::chrono::high_resolution_clock::now();
#endif // TIMER

      it->Refresh();
      assert(it->status().ok());
      for (it->Seek(start_key); it->Valid(); it->Next()) {
        if (it->key().ToString() >= end_key) {
          break;
        }
        keys_returned++;
      }
      if (!it->status().ok()) {
        (*buffer) << it->status().ToString() << std::endl << std::flush;
      }
#ifdef TIMER
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
      (*stats) << "ScanTime: " << duration.count() << std::endl;
      rq_exec_time += duration.count();
#endif // TIMER
      break;
    }
    default:
      (*buffer) << "ERROR: Case match NOT found !!" << std::endl;
      break;
    }

    ith_op += 1;
    UpdateProgressBar(env, ith_op, total_operations,
                      (int)total_operations * 0.02);
    if (is_last_line)
      break;
  }

#ifdef PROFILE
  (*buffer) << "=====================" << std::endl;
  LogTreeState(db, buffer);
  LogRocksDBStatistics(db, options, buffer);
#endif // PROFILE

  auto total_exec_time =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::high_resolution_clock::now() - exec_start)
          .count();
#ifdef TIMER
  (*buffer) << "=====================" << std::endl;
  (*buffer) << "Workload Execution Time: " << total_exec_time << std::endl;
  (*buffer) << "Inserts Execution Time: " << inserts_exec_time << std::endl;
  (*buffer) << "Updates Execution Time: " << updates_exec_time << std::endl;
  (*buffer) << "PointQuery Execution Time: " << pq_exec_time << std::endl;
  (*buffer) << "PointDelete Execution Time: " << pdelete_exec_time << std::endl;
  (*buffer) << "RangeQuery Execution Time: " << rq_exec_time << std::endl;
#endif // TIMER

  // print global stat we collected
  DB::PrintCurStat();
  // delete iterator and close db
  delete it;
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());
  s = db->Close();
  if (!s.ok())
    std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  PrintRocksDBPerfStats(env, buffer, options);
  table_options.block_cache.reset();
  options.table_factory.reset();
  (*buffer) << "===========END HERE=========\n";

  // flush final stats and delete ptr
  buffer->flush();
  stats->flush();
  long long total_seconds = total_exec_time / 1e9;
  std::cout << "Experiment completed in " << total_seconds / 3600 << "h "
            << (total_seconds % 3600) / 60 << "m " << total_seconds % 60 << "s "
            << std::endl;
  return 0;
}