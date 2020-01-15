#ifndef COMMON_H
#define COMMON_H

#include <algorithm>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/flight/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
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

#endif  // COMMON_H