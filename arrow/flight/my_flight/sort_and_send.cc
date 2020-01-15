#include <gflags/gflags.h>

#include "sender.h"
#include "sorter.h"

DEFINE_string(input_file, "", "The input file containing data to be sorted.");
DEFINE_string(server_hosts, "localhost", "Host(s) where the server(s) is/are running on");
DEFINE_int32(server_port, 30103, "The port to connect to");
DEFINE_string(partition_boundaries, "GROUP9,GROUP19",
              "The boundaries of the partitioned records");
DEFINE_bool(debug_mode, false, "If on, more info will be put to stdout");

std::shared_ptr<arrow::RecordBatch> ConvertStructVectorToRecordBatch(
    const std::vector<Record>& records) {
  // Hard-coded for now; can be derived from struct Record
  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("group_name", arrow::utf8()), arrow::field("seq", arrow::int64()),
       arrow::field("data", arrow::utf8())});

  int64_t records_size = static_cast<int64_t>(records.size());

  std::vector<std::string> group_name_vector;
  int64_t seq_array[records_size];
  std::vector<std::string> data_vector;

  for (int64_t i = 0; i < records_size; ++i) {
    group_name_vector.push_back(records[i].group_name);
    seq_array[i] = records[i].seq;
    data_vector.push_back(records[i].data);
  }

  arrow::StringBuilder group_name_builder;
  arrow::Int64Builder seq_builder;
  arrow::StringBuilder data_builder;
  ABORT_NOT_OK(group_name_builder.AppendValues(group_name_vector));
  ABORT_NOT_OK(seq_builder.AppendValues(seq_array, records_size));
  ABORT_NOT_OK(data_builder.AppendValues(data_vector));

  std::shared_ptr<arrow::StringArray> group_name_col;
  std::shared_ptr<arrow::Int64Array> seq_col;
  std::shared_ptr<arrow::StringArray> data_col;
  ABORT_NOT_OK(group_name_builder.Finish(&group_name_col));
  ABORT_NOT_OK(seq_builder.Finish(&seq_col));
  ABORT_NOT_OK(data_builder.Finish(&data_col));

  std::vector<std::shared_ptr<arrow::Array>> batch_cols;
  batch_cols.push_back(group_name_col);
  batch_cols.push_back(seq_col);
  batch_cols.push_back(data_col);

  std::shared_ptr<arrow::RecordBatch> record_batch =
      arrow::RecordBatch::Make(schema, records_size, batch_cols);

  return record_batch;
}

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

  // Partition the sorted records
  std::list<std::string> boundaries =
      SeparatePartitionBoundaries(FLAGS_partition_boundaries);
  auto first = records.begin();
  auto last = records.end();
  std::vector<Record> temp_rec_vec;
  std::vector<std::vector<Record>> sub_records;
  std::string cur_bound = boundaries.front();
  boundaries.pop_front();
  for (auto it = records.begin(); it != records.end(); ++it) {
    if (it->group_name == cur_bound && (it + 1)->group_name != cur_bound) {
      last = it;
      temp_rec_vec = {first, last + 1};  // seems to be [,)
      sub_records.push_back(temp_rec_vec);
      first = last + 1;  // first of next, +1 of current last
      if (boundaries.size() != 0) {
        cur_bound = boundaries.front();
        boundaries.pop_front();
      } else {
        break;
      }
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
