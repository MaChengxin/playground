#include <gflags/gflags.h>

#include <arrow/table.h>
#include "common.h"

DEFINE_string(server_host, "localhost", "Host where the server is running on");
DEFINE_int32(server_port, 30103, "Server port to listen on");
DEFINE_bool(debug_mode, false, "If on, more info will be put to stdout");

class MyFlightServer : public arrow::flight::FlightServerBase {
  arrow::Status DoPut(
      const arrow::flight::ServerCallContext& context,
      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
      std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {
    std::vector<std::shared_ptr<arrow::RecordBatch>> retrieved_chunks;
    arrow::flight::FlightStreamChunk chunk;
    while (true) {
      // Question: what is the capacity of a chunk?
      RETURN_NOT_OK(reader->Next(&chunk));
      if (!chunk.data) break;
      retrieved_chunks.push_back(chunk.data);
      if (chunk.app_metadata) {
        std::cout << "chunk.app_metadata" << chunk.app_metadata->ToString() << std::endl;
        RETURN_NOT_OK(writer->WriteMetadata(*chunk.app_metadata));
      }
    }

    if (FLAGS_debug_mode) {
      auto record_batch = retrieved_chunks.back();
      std::cout << "Column 2 of the received Record Batch: \n"
                << record_batch->column(2)->ToString() << std::endl;
      std::cout << "Number of columns of the received Record Batch: "
                << record_batch->num_columns() << std::endl;
      std::cout << "Number of rows of the received Record Batch: "
                << record_batch->num_rows() << std::endl;
      std::cout << "Schema of the received Record Batch: \n"
                << record_batch->schema()->ToString() << std::endl;
    }

    std::shared_ptr<arrow::Table> received_data;
    RETURN_NOT_OK(arrow::Table::FromRecordBatches(retrieved_chunks, &received_data));

    std::cout << "Number of columns of received data: " << received_data->num_columns()
              << std::endl;
    std::cout << "Number of rows of received data: " << received_data->num_rows()
              << std::endl;
    std::cout << "Schema of received data: \n"
              << received_data->schema()->ToString() << std::endl;

    do_put_counter += 1;
    std::cout << "Number of times DoPut has been called for: " << do_put_counter
              << std::endl;

    all_received_data[std::to_string(do_put_counter)] = received_data;

    return arrow::Status::OK();
  }

 private:
  int do_put_counter = 0;
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> all_received_data;
};

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<MyFlightServer> my_flight_server;
  my_flight_server.reset(new MyFlightServer);

  // According to Flight.proto: a location is where a Flight service will accept
  // retrieval of a particular stream given a ticket.
  arrow::flight::Location location;
  ARROW_CHECK_OK(
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_server_port, &location));
  std::cout << "Server location: " << location.ToString() << std::endl;
  arrow::flight::FlightServerOptions options(location);

  ARROW_CHECK_OK(my_flight_server->Init(options));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(my_flight_server->SetShutdownOnSignals({SIGTERM}));
  std::cout << "Server host: " << FLAGS_server_host << std::endl;
  std::cout << "Server port: " << FLAGS_server_port << std::endl;
  ARROW_CHECK_OK(my_flight_server->Serve());
  return 0;
}
