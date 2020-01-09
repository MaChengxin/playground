#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <plasma/client.h>

struct Record {
  std::string group_name;
  int64_t seq;
  std::string data;
};

bool CompareRecords(const Record& a, const Record& b);
std::shared_ptr<arrow::RecordBatch> ConvertStructVectorToRecordBatch(
    const std::vector<Record>& records);
