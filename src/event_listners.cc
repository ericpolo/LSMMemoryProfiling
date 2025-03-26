#include "event_listners.h"

std::mutex mtx;
std::condition_variable cv;
bool compaction_complete = false;

void WaitForCompactions(DB *db) {
  std::unique_lock<std::mutex> lock(mtx);
  uint64_t num_running_compactions;
  uint64_t pending_compaction_bytes;
  uint64_t num_pending_compactions;

  while (!compaction_complete) {
    // Check if there are ongoing or pending compactions
    db->GetIntProperty("rocksdb.num-running-compactions",
                       &num_running_compactions);
    db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes",
                       &pending_compaction_bytes);
    db->GetIntProperty("rocksdb.compaction-pending", &num_pending_compactions);
    if (num_running_compactions == 0 && pending_compaction_bytes == 0 &&
        num_pending_compactions == 0) {
      break;
    }
    cv.wait_for(lock, std::chrono::milliseconds(10));
  }
}

void FlushListner::OnFlushCompleted(DB* db, const FlushJobInfo& fji) {
  if (buffer_ != nullptr) {
    // we are running normal workload
    (*buffer_) << "buffer is full, flush finished info [num_entries]: " << fji.table_properties.num_entries;
    (*buffer_) << "[Flush Stats] raw_key_size: " << fji.table_properties.raw_key_size
              << ", raw_value_size: " << fji.table_properties.raw_value_size << std::endl;
  } else {
    // we are running sample workload and testing the flush time.
    if (job_start_time.find(fji.job_id) != job_start_time.end()) {
      job_end_time[fji.job_id] = std::chrono::high_resolution_clock::now();
    }
  }
}


void FlushListner::OnFlushBegin(DB* db, const FlushJobInfo& fji) {
  if (buffer_ == nullptr) {
    // we are running sample workload and testing the flush time.
    if (job_start_time.find(fji.job_id) == job_start_time.end()) {
      job_start_time[fji.job_id] = std::chrono::high_resolution_clock::now();
    }
  }
}
