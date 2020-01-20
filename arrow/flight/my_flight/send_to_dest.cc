#include <gflags/gflags.h>
#include <boost/asio/ip/host_name.hpp>

#include "common.h"
#include "sender.h"

DEFINE_string(server_hosts, "localhost", "Host(s) where the server(s) is/are running on");
DEFINE_int32(server_port, 30103, "The port to connect to");

std::string ConvertObjectIDString(std::string forty_chars) {
  std::string twenty_chars;
  int first;
  int second;
  for (int i = 0; i < 39; i = i + 2) {
    first = std::stoi(forty_chars.substr(i, 1), nullptr, 16);
    second = std::stoi(forty_chars.substr(i + 1, 1), nullptr, 16);
    char new_char = (first << 4) + second;
    twenty_chars += new_char;
  }
  return twenty_chars;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  auto host_name = boost::asio::ip::host_name();
  std::ofstream log_file;
  log_file.open(host_name + "_s.log", std::ios_base::app);
  log_file << PrettyPrintCurrentTime() << "send-to-dest started" << std::endl;

  std::vector<std::string> server_hosts = SeparateServerHosts(FLAGS_server_hosts);

  // Get the Object IDs
  std::vector<plasma::ObjectID> object_ids;
  std::vector<std::string> object_id_strs;
  std::ifstream in_file(host_name + "_object_ids.txt");
  std::string object_id_40_chars;
  while (in_file >> object_id_40_chars) {
    object_id_strs.push_back(ConvertObjectIDString(object_id_40_chars));
  }

  for (auto object_id_str : object_id_strs) {
    object_ids.push_back(plasma::ObjectID::from_binary(object_id_str));
  }

  arrow::Status comm_status;

  // TODO: make this parallelized
  int i = 0;
  for (auto const& server_host : server_hosts) {
    log_file << PrettyPrintCurrentTime() << "Started sending to " << server_host
             << std::endl;
    comm_status = SendToDestinationNode(server_host, FLAGS_server_port,
                                        object_ids[i]);  // better to use a map here
    i = i + 1;
    log_file << PrettyPrintCurrentTime() << "Finished sending to " << server_host
             << std::endl;
    if (!comm_status.ok()) {
      std::cerr << "Sending to responsible node failed with error: "
                << comm_status.ToString() << std::endl;
    }
  }

  log_file << PrettyPrintCurrentTime() << "send-to-dest finished" << std::endl;

  return 0;
}
