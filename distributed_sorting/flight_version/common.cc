#include "common.h"

std::string PrettyPrintCurrentTime() {
  std::time_t cur_time =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::string time_str(std::ctime(&cur_time));

  return "[" + time_str.substr(0, time_str.size() - 1) + "]: ";  // substr to remove \n
}