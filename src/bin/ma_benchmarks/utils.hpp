#pragma once

#include "types.hpp"
#include "operators/export_csv.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/iterables/create_iterable_from_column.hpp"
#include "storage/index/counting_quotient_filter/counting_quotient_filter.hpp"
#include "storage/storage_manager.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/current_scheduler.hpp"

#include <stdlib.h>
#include <pwd.h>
#include <stdio.h>

using namespace opossum;

std::string data_type_to_string(DataType data_type) {
  if (data_type == DataType::Int) {
    return "int";
  } else if (data_type == DataType::Double) {
    return "double";
  } else if (data_type == DataType::String) {
    return "string";
  } else {
    return "unknown";
  }
}

/**
* Gets the linux user name of the current user.
**/
std::string getUserName()
{
  uid_t uid = geteuid();
  struct passwd *pw = getpwuid(uid);
  if (pw) {
    return std::string(pw->pw_name);
  }

  return "";
}

void serialize_results_csv(std::string benchmark_name, std::shared_ptr<Table> table) {
  std::cout << "Writing results to csv...";
  auto file_name = "/home/" + getUserName() + "/dev/MasterarbeitJupyter/" + benchmark_name + "_results.csv";
  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();
  auto export_csv = std::make_shared<ExportCsv>(table_wrapper, file_name);
  export_csv->execute();
  std::cout << "OK!" << std::endl;
}

void clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

void print_chunk_sizes(std::shared_ptr<const Table> table) {
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); chunk_id++) {
    auto chunk = table->get_chunk(chunk_id);
    std::cout << "Chunk " << chunk_id << " size: " << chunk->size() << std::endl;
  }
}

void create_quotient_filters(std::shared_ptr<Table> table, ColumnID column_id, uint8_t quotient_size,
    	                       uint8_t remainder_size) {
  if (remainder_size > 0 && quotient_size > 0) {
    auto filter_insert_jobs = table->populate_quotient_filters(column_id, quotient_size, remainder_size);
    for (auto job : filter_insert_jobs) {
      job->schedule();
    }
    CurrentScheduler::wait_for_tasks(filter_insert_jobs);
  } else {
    table->delete_quotient_filters(column_id);
  }
}
