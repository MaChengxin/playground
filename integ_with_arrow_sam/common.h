#ifndef COMMON_H
#define COMMON_H

#include <algorithm>
#include <chrono>
#include <csignal>
#include <ctime>
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

std::string PrettyPrintCurrentTime();

#endif  // COMMON_H