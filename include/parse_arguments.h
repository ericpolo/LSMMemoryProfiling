#include <iostream>

#include "args.hxx"
#include "db_env.h"

int parse_arguments(int argc, char *argv[], std::unique_ptr<DBEnv> &env) {
  args::ArgumentParser parser("RocksDB_parser.", "");
  args::Group group1(parser, "This group is all exclusive:",
                     args::Group::Validators::DontCare);

  args::ValueFlag<int> destroy_database_cmd(
      group1, "d", "Destroy and recreate the database [def: 1]",
      {'d', "destroy"});
  args::ValueFlag<int> clear_system_cache_cmd(
      group1, "cc", "Clear system cache [def: 1]", {"cc"});

  args::ValueFlag<int> size_ratio_cmd(
      group1, "T",
      "The number of unique inserts to issue in the experiment [def: 10]",
      {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(
      group1, "P",
      "The number of unique inserts to issue in the experiment [def: 512]",
      {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(
      group1, "B",
      "The number of unique inserts to issue in the experiment [def: 4]",
      {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(
      group1, "E",
      "The number of unique inserts to issue in the experiment [def: 1024 B]",
      {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(
      group1, "M",
      "The number of unique inserts to issue in the experiment [def: 16 MB]",
      {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(
      group1, "file_to_memtable_size_ratio",
      "The number of unique inserts to issue in the experiment [def: 1]",
      {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(
      group1, "file_size",
      "The number of unique inserts to issue in the experiment [def: 256 KB]",
      {'F', "file_size"});
  args::ValueFlag<int> compaction_pri_cmd(
      group1, "compaction_pri",
      "[Compaction priority: 1 for kMinOverlappingRatio, 2 for "
      "kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for "
      "kOldestSmallestSeqFirst; def: 1]",
      {'c', "compaction_pri"});
  args::ValueFlag<int> compaction_style_cmd(
      group1, "compaction_style",
      "[Compaction priority: 1 for kCompactionStyleLevel, 2 for "
      "kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for "
      "kCompactionStyleNone; def: 1]",
      {'C', "compaction_style"});
  args::ValueFlag<int> bits_per_key_cmd(
      group1, "bits_per_key",
      "The number of bits per key assigned to Bloom filter [def: 10]",
      {'b', "bits_per_key"});
  args::ValueFlag<int> block_cache_cmd(
      group1, "bb", "Block cache size in MB [def: 8 MB]", {"bb"});
  args::ValueFlag<int> enable_perf_iostat_cmd(
      group1, "enable_perf_iostat",
      "Enable RocksDB's internal Perf and IOstat [def: 0]", {"stat"});
  args::ValueFlag<int> show_progress_cmd(
      group1, "show_progress_bar", "Shows progress bar [def: 0]", {"progress"});

  // LSMMemoryProfiling
  args::ValueFlag<long> num_inserts_cmd(
      group1, "inserts",
      "The number of unique inserts to issue in the experiment [def: 1]",
      {'I', "inserts"});
  args::ValueFlag<long> num_updates_cmd(
      group1, "updates",
      "The number of unique updates to issue in the experiment [def: 0]",
      {'U', "updates"});
  args::ValueFlag<long> num_range_queries_cmd(
      group1, "range_queries",
      "The number of unique range queries to issue in the experiment [def: 0]",
      {'S', "range_queries"});
  args::ValueFlag<float> range_query_selectivity_cmd(
      group1, "Y", "Range query selectivity [def: 0.1]",
      {'Y', "range_query_selectivity"});

  args::ValueFlag<int> memtable_factory_cmd(
      group1, "memtable_factory",
      "[Memtable Factory: 1 for Skiplist, 2 for Vector, 3 for Hash Skiplist, 4 "
      "for Hash Linkedlist; 5 for Unsorted Vector def: 1]",
      {'m', "memtable_factory"});
  args::ValueFlag<int> prefix_length_cmd(
      group1, "prefix_length",
      "[Prefix Length: Number of bytes of the key forming the prefix; def: 0]",
      {'X', "prefix_length"});
  args::ValueFlag<long> bucket_count_cmd(
      group1, "bucket_count",
      "[Bucket Count: Number of buckets for the hash table in HashSkipList & "
      "HashLinkList Memtables; def: 50000]",
      {'H', "bucket_count"});
  args::ValueFlag<long> threshold_use_skiplist_cmd(
      group1, "threshold_use_skiplist",
      "[Threshold Use SkipList: Threshold based on which the conversion will "
      "happen from HashLinkList to HashSkipList; def: 256]",
      {"threshold_use_skiplist", "threshold_use_skiplist"});
  args::ValueFlag<long> vector_pre_allocation_size_cmd(
      group1, "preallocation_vector_size",
      "[Preallocation Vector Size: Size to preallocation to vector memtable; "
      "def: 0]",
      {'A', "preallocation_size"});

  args::ValueFlag<int> run_sample_workload_cmd(
    group1, "run_sample_workload",
    "[Run 848 Project sample workload or not: 0 for No, 1 for Yes; "
    "def: 0]",
    {'s', "sample_workload"});

  args::ValueFlag<int> kv_entry_size_cmd(
    group1, "kv_entry_size",
    "[Entry size in bytes;"
    "def: 8]",
    {'e', "kv_entry_size"});

  args::ValueFlag<float> key_value_size_ratio_cmd(
    group1, "key_value_size_ratio",
    "[Ratio of key size for each entry;"
    "def: 0.5]",
    {'r', "key_value_size_ratio"});

  args::ValueFlag<int> num_kv_entries_cmd(
    group1, "num_kv_entries",
    "[Number of kv entries will be generated in the sample workload;"
    "def: 20000]",
    {'n', "num_kv_entries"});

  try {
    parser.ParseCLI(argc, argv);
  } catch (args::Help &) {
    std::cout << parser;
    exit(0);
  } catch (args::ParseError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  } catch (args::ValidationError &e) {
    std::cerr << e.what() << std::endl;
    std::cerr << parser;
    return 1;
  }

  env->SetDestroyDatabase(destroy_database_cmd
                              ? args::get(destroy_database_cmd)
                              : env->IsDestroyDatabaseEnabled());
  env->clear_system_cache = clear_system_cache_cmd
                                ? args::get(clear_system_cache_cmd)
                                : env->clear_system_cache;
  env->size_ratio =
      size_ratio_cmd ? args::get(size_ratio_cmd) : env->size_ratio;
  env->buffer_size_in_pages = buffer_size_in_pages_cmd
                                  ? args::get(buffer_size_in_pages_cmd)
                                  : env->buffer_size_in_pages;
  env->entries_per_page = entries_per_page_cmd ? args::get(entries_per_page_cmd)
                                               : env->entries_per_page;
  env->entry_size =
      entry_size_cmd ? args::get(entry_size_cmd) : env->entry_size;
  env->SetBufferSize(buffer_size_cmd ? args::get(buffer_size_cmd) : 0);
  env->file_to_memtable_size_ratio =
      file_to_memtable_size_ratio_cmd
          ? args::get(file_to_memtable_size_ratio_cmd)
          : env->file_to_memtable_size_ratio;
  env->compaction_pri =
      compaction_pri_cmd ? args::get(compaction_pri_cmd) : env->compaction_pri;
  env->compaction_style = compaction_style_cmd ? args::get(compaction_style_cmd)
                                               : env->compaction_style;
  env->bits_per_key =
      bits_per_key_cmd ? args::get(bits_per_key_cmd) : env->bits_per_key;
  env->block_cache =
      block_cache_cmd ? args::get(block_cache_cmd) : env->block_cache;
  env->SetPerfIOStat(enable_perf_iostat_cmd ? args::get(enable_perf_iostat_cmd)
                                            : env->IsPerfIOStatEnabled());
  env->SetShowProgress(show_progress_cmd ? args::get(show_progress_cmd)
                                         : env->IsShowProgressEnabled());

  // LSM options
  env->num_inserts =
      num_inserts_cmd ? args::get(num_inserts_cmd) : env->num_inserts;
  env->num_updates =
      num_updates_cmd ? args::get(num_updates_cmd) : env->num_updates;
  env->num_range_queries = num_range_queries_cmd
                               ? args::get(num_range_queries_cmd)
                               : env->num_range_queries;

  env->memtable_factory = memtable_factory_cmd ? args::get(memtable_factory_cmd)
                                               : env->memtable_factory;
  env->prefix_length =
      prefix_length_cmd ? args::get(prefix_length_cmd) : env->prefix_length;
  env->bucket_count =
      bucket_count_cmd ? args::get(bucket_count_cmd) : env->bucket_count;
  env->linklist_threshold_use_skiplist =
      threshold_use_skiplist_cmd ? args::get(threshold_use_skiplist_cmd)
                                 : env->linklist_threshold_use_skiplist;
  env->vector_preallocation_size_in_bytes =
      vector_pre_allocation_size_cmd ? args::get(vector_pre_allocation_size_cmd)
                                     : env->vector_preallocation_size_in_bytes;

  if (run_sample_workload_cmd) {
    env->run_sample_workload = args::get(run_sample_workload_cmd) == 0? false:true;
  }
  env->kv_entry_size = kv_entry_size_cmd? args::get(kv_entry_size_cmd):env->kv_entry_size;
  env->key_value_size_ratio = key_value_size_ratio_cmd? args::get(key_value_size_ratio_cmd):env->key_value_size_ratio;
  env->num_kv_entries = num_kv_entries_cmd? args::get(num_kv_entries_cmd): env->num_kv_entries;
  env->range_query_selectivity = range_query_selectivity_cmd? args::get(range_query_selectivity_cmd): env->range_query_selectivity;

  return 0;
}
