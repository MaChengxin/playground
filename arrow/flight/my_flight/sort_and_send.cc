#include <gflags/gflags.h>

#include "sender.h"
#include "sorter.h"

DEFINE_string(input_file, "", "The input file containing data to be sorted.");
DEFINE_string(server_hosts, "localhost", "Host(s) where the server(s) is/are running on");
DEFINE_int32(server_port, 30103, "The port to connect to");
DEFINE_bool(debug_mode, false, "If on, more info will be put to stdout");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Read the input file, construct the data to be sorted
  std::vector<Record> records;

  std::ifstream in_file(FLAGS_input_file);
  std::string group, seq, data;
  while (in_file >> group >> seq >> data) {
    records.push_back({group, std::stoi(seq), data});
  }
  if (FLAGS_debug_mode) {
    std::cout << "Before sorting" << std::endl;
    for (auto const& rec : records) {
      std::cout << rec.group_name << "\t" << rec.seq << "\t" << rec.data << std::endl;
    }
  }

  // Sort the data
  std::sort(records.begin(), records.end(), CompareRecords);
  if (FLAGS_debug_mode) {
    std::cout << "After sorting" << std::endl;
    for (auto const& rec : records) {
      std::cout << rec.group_name << "\t" << rec.seq << "\t" << rec.data << std::endl;
    }
  }

  // Partition the records
  auto first = records.begin();
  auto last = records.end();
  std::vector<Record> temp_rec_vec;
  std::vector<std::vector<Record>> sub_records;
  for (auto it = records.begin(); it != records.end(); ++it) {
    if (it->group_name == "GROUP9" && (it + 1)->group_name == "GROUP10") {
      last = it;
      temp_rec_vec = {first, last + 1};  // seems to be [,)
      sub_records.push_back(temp_rec_vec);
      first = last + 1;  // first of next, +1 of current last
    }

    if (it->group_name == "GROUP19" && (it + 1)->group_name == "GROUP20") {
      last = it;
      temp_rec_vec = {first, last + 1};
      sub_records.push_back(temp_rec_vec);
      first = last + 1;
      break;
    }
  }
  temp_rec_vec = {first, records.end()};
  sub_records.push_back(temp_rec_vec);

  // Send away the sub records
  arrow::Status comm_status;
  std::vector<std::string> server_hosts = SeparateServerHosts(FLAGS_server_hosts);

  // TODO: make this parallelized
  int i = 0;
  for (auto const& server_host : server_hosts) {
    comm_status =
        SendToResponsibleNode(server_host, FLAGS_server_port,
                              *ConvertStructVectorToRecordBatch(sub_records[i]));
    i = i + 1;
    if (!comm_status.ok()) {
      std::cerr << "Sending to responsible node failed with error: "
                << comm_status.ToString() << std::endl;
    }
  }

  return 0;
}
