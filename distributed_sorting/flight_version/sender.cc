#include "sender.h"
#include "in_memory_storage.h"

arrow::Status SendToDestinationNode(std::string host, int port,
                                    plasma::ObjectID object_id) {
  /* Get the records from Plasma */
  // Start up and connect a Plasma client
  plasma::PlasmaClient plasma_client;
  ARROW_CHECK_OK(plasma_client.Connect("/tmp/plasma"));
  std::shared_ptr<arrow::RecordBatch> record_batch;
  record_batch = GetRecordBatchFromPlasma(object_id, plasma_client);

  std::unique_ptr<arrow::flight::FlightClient> client;
  arrow::flight::Location location;
  ABORT_NOT_OK(arrow::flight::Location::ForGrpcTcp(host, port, &location));
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

  std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
  std::unique_ptr<arrow::flight::FlightMetadataReader> reader;
  arrow::Status do_put_status = client->DoPut(arrow::flight::FlightDescriptor{},
                                              record_batch->schema(), &writer, &reader);
  if (!do_put_status.ok()) {
    return do_put_status;
  }

  arrow::Status writer_status;
  writer_status = writer->WriteRecordBatch(*record_batch);
  writer_status = writer->Close();
  if (!writer_status.ok()) {
    return writer_status;
  }

  // Disconnect the client
  ARROW_CHECK_OK(plasma_client.Disconnect());

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
