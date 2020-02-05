#include "sorter.h"

bool CompareGroupName(const std::string& a, const std::string& b) {
  std::string group_str = "GROUP";
  std::string a_substr = a.substr(group_str.length());
  int a_substr_int = std::stoi(a_substr);
  std::string b_substr = b.substr(group_str.length());
  int b_substr_int = std::stoi(b_substr);
  return a_substr_int < b_substr_int;
}

/* Source:
 * https://stackoverflow.com/questions/3574680/sort-based-on-multiple-things-in-c*/
bool CompareRecords(const Record& a, const Record& b) {
  if (CompareGroupName(a.group_name, b.group_name)) return true;
  if (CompareGroupName(b.group_name, a.group_name)) return false;

  // a == b for primary condition, go to secondary condition
  if (a.seq < b.seq) return true;
  if (b.seq < a.seq) return false;

  return false;
}
