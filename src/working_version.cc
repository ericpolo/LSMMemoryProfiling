/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */
#include <memory>

#include <db_env.h>
#include <parse_arguments.h>
#include <run_workload.h>
#include <sample_workload.h>

int main(int argc, char *argv[]) {
  std::unique_ptr<DBEnv> env = DBEnv::GetInstance();

  if (parse_arguments(argc, argv, env)) {
    std::cerr << "Failed to parse arguments. Exiting." << std::endl;
    return 1;
  }

  if (env->run_sample_workload) {
    printf("Running CS848 Sample Workload....\n");
    return runSampleWorkload(env);
  } else {
    printf("Running Workload....\n");
    return runWorkload(env);
  }
}