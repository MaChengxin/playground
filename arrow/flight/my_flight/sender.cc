#include "sender.h"

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
