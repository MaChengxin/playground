/*
This client is capable of sending record batches to multiple servers.
*/

#include <iostream>
#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/flight/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/logging.h>
#include <plasma/client.h>

DEFINE_string(server_hosts, "localhost", "Host(s) where the server(s) is/are running on");
DEFINE_int32(server_port, 30103, "The port to connect to");

std::shared_ptr<arrow::RecordBatch> MakeMyRecordBatch(
    const std::shared_ptr<arrow::Schema>& schema, int64_t batch_num_rows) {
  int64_t raw_array[batch_num_rows];
  for (int i = 0; i < batch_num_rows; ++i) {
    raw_array[i] = i;
  }

  arrow::Int64Builder builder;
  ABORT_NOT_OK(builder.AppendValues(raw_array, batch_num_rows, nullptr));

  std::shared_ptr<arrow::Int64Array> batch_col;
  ABORT_NOT_OK(builder.Finish(&batch_col));

  std::vector<std::shared_ptr<arrow::Array>> batch_cols;

  const int32_t num_cols = schema->num_fields();
  arrow::Status make_batch_cols_status;
  for (int i = 0; i < num_cols; ++i) {
    batch_cols.push_back(batch_col);
    make_batch_cols_status = batch_cols.back()->Validate();
  }

  if (!make_batch_cols_status.ok()) {
    std::cerr << "Making columns of the Record Batch failed with error: << "
              << make_batch_cols_status.ToString() << std::endl;
  }

  std::shared_ptr<arrow::RecordBatch> record_batch =
      arrow::RecordBatch::Make(schema, batch_num_rows, batch_cols);

  return record_batch;
}

arrow::Status SendToResponsibleNode(std::string host, int port,
                                    const arrow::RecordBatch& record_batch) {
  std::unique_ptr<arrow::flight::FlightClient> client;
  arrow::flight::Location location;
  ABORT_NOT_OK(arrow::flight::Location::ForGrpcTcp(host, port, &location));
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> reader;

  arrow::Status do_put_status = client->DoPut(arrow::flight::FlightDescriptor{},
                                              record_batch.schema(), &writer, &reader);
  if (!do_put_status.ok()) {
    return do_put_status;
  }

  arrow::Status writer_status;
  writer_status = writer->WriteRecordBatch(record_batch);
  writer_status = writer->Close();
  if (!writer_status.ok()) {
    return writer_status;
  }

  return arrow::Status::OK();
}

/* Source:
 * https://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c*/
std::vector<std::string> SeparateServerHosts(std::string server_hosts) {
  std::string s = server_hosts;
  std::string delimiter = ",";
  std::vector<std::string> server_hosts_vec;

  size_t pos = 0;
  std::string token;
  while ((pos = s.find(delimiter)) != std::string::npos) {
    token = s.substr(0, pos);
    server_hosts_vec.push_back(token);
    s.erase(0, pos + delimiter.length());
  }
  server_hosts_vec.push_back(s);
  return server_hosts_vec;
}

void GetRecordBatchFromPlasma(plasma::ObjectID object_id,
                              std::shared_ptr<arrow::RecordBatch>& record_batch) {}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  /* NOTE: putting the functionality of retrieving Record Batch from Plasma in a function
   * leads to problem which I am unable to solve at the moment. The problem is that by
   * doing that, the meta info of the retrieved Record Batch is readable, but the content
   * (i.e. the values in the columns) is not available and the program terminates with a
   * segmentation fault. */
  /* === Start retrieving Record Batch from Plasma === */
  // Start up and connect a Plasma client
  plasma::PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/plasma"));
  plasma::ObjectID object_id = plasma::ObjectID::from_binary("0FF1CE00C0FFEE00BEEF");
  plasma::ObjectBuffer object_buffer;
  ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

  // Retrieve object data
  auto buffer = object_buffer.data;

  arrow::io::BufferReader buffer_reader(
      buffer);  // BufferReader is a derived class of arrow::io::InputStream
  std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
  ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader,
                                                           &record_batch_stream_reader));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrow::Status status;
  status = record_batch_stream_reader->ReadNext(&record_batch);
  /* === End retrieving Record Batch from Plasma === */

  std::cout << "Schema of the retrieved Record Batch: \n"
            << record_batch->schema()->ToString() << std::endl;
  std::cout << "Number of columns of the retrieved Record Batch: "
            << record_batch->num_columns() << std::endl;
  std::cout << "Number of rows of the retrieved Record Batch: "
            << record_batch->num_rows() << std::endl;
  std::cout << "Column 0 of the retrieved Record Batch: \n"
            << record_batch->column(0)->ToString() << std::endl;
  std::cout << "Column 1 of the retrieved Record Batch: \n"
            << record_batch->column(1)->ToString() << std::endl;
  std::cout << "Column 2 of the retrieved Record Batch: \n"
            << record_batch->column(2)->ToString() << std::endl;

  arrow::Status comm_status;
  std::vector<std::string> server_hosts = SeparateServerHosts(FLAGS_server_hosts);

  // TODO: make this parallelized
  for (auto const& server_host : server_hosts) {
    comm_status = SendToResponsibleNode(server_host, FLAGS_server_port, *record_batch);
    if (!comm_status.ok()) {
      std::cerr << "Sending to responsible node failed with error: "
                << comm_status.ToString() << std::endl;
    }
  }

  return 0;
}
