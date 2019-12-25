/*
Assuming one client and one server now.
In the final implementation, there will be more nodes and each node is both a client and a
server (all-to-all communication pattern).
TODO: Send first half of the Record Batch to one server, and the second half to another
*/

#include <iostream>
#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/flight/api.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"

DEFINE_string(server_host, "localhost", "Host where the server is running on");
DEFINE_int32(server_port, 30103, "The port to connect to");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<arrow::flight::FlightClient> client;
  arrow::flight::Location location;
  ABORT_NOT_OK(arrow::flight::Location::ForGrpcTcp(FLAGS_server_host, FLAGS_server_port,
                                                   &location));
  // std::cout << "Server location: " << location.ToString() << std::endl;
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

  std::cout << "Goed, connection didn't fail." << std::endl;

  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> reader;
  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
       arrow::field("c", arrow::int64()), arrow::field("d", arrow::int64())});
  arrow::Status do_put_status =
      client->DoPut(arrow::flight::FlightDescriptor{}, schema, &writer, &reader);
  if (!do_put_status.ok()) {
    std::cerr << "Client DoPut failed with error: << " << do_put_status.ToString()
              << std::endl;
  }

  const int64_t batch_num_rows = 42;

  int64_t raw_array[batch_num_rows];
  for (int i = 0; i < batch_num_rows; ++i) {
    raw_array[i] = i;
  }

  arrow::Int64Builder builder;

  ABORT_NOT_OK(builder.AppendValues(raw_array, batch_num_rows, nullptr));

  std::shared_ptr<arrow::Int64Array> batch_col;
  ABORT_NOT_OK(builder.Finish(&batch_col));

  std::vector<std::shared_ptr<arrow::Array>> batch_cols;

  const int32_t num_cols = 4;
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

  std::cout << "Column 2 of the full Record Batch: \n" << record_batch->column(2)->ToString() << std::endl;
  std::cout << "Number of columns of the full Record Batch: " << record_batch->num_columns() << std::endl;
  std::cout << "Number of rows of the full Record Batch: " << record_batch->num_rows() << std::endl;
  std::cout << "Schema of the full Record Batch: \n" << record_batch->schema()->ToString() << std::endl;

  std::shared_ptr<arrow::RecordBatch> half_record_batch = record_batch->Slice(0, batch_num_rows / 2);

  arrow::Status writer_status;

  writer_status = writer->WriteRecordBatch(*half_record_batch);
  writer_status = writer->Close();

  if (!writer_status.ok()) {
    std::cerr << "Writer failed with error: << " << writer_status.ToString() << std::endl;
  }

  return 0;
}