#ifndef RUN_SAMPLE_WORKLOAD_H_
#define RUN_SAMPLE_WORKLOAD_H_

#include <memory>

#include "db_env.h"

extern std::string kDBPath;
extern std::string buffer_file;

int runSampleWorkload(std::unique_ptr<DBEnv> &env);

#endif // RUN_WORKLOAD_H_