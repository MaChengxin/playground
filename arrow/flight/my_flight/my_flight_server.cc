#include <csignal>
#include <iostream>
#include <memory>
#include <vector>

#include <gflags/gflags.h>

#include "arrow/api.h"
#include "arrow/flight/api.h"
#include "arrow/record_batch.h"
#include "arrow/util/logging.h"

DEFINE_string(server_host, "localhost", "Host where the server is running on");
DEFINE_int32(server_port, 30103, "Server port to listen on");

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

    auto rb = retrieved_chunks.back();
    std::cout << "Column 2 of the received Record Batch: \n" << rb->column(2)->ToString() << std::endl;
    std::cout << "Number of columns of the received Record Batch: " << rb->num_columns() << std::endl;
    std::cout << "Number of rows of the received Record Batch: " << rb->num_rows() << std::endl;
    std::cout << "Schema of the received Record Batch: \n" << rb->schema()->ToString() << std::endl;

    // TODO: Put the received data into Plasma Object Store

    return arrow::Status::OK();
  }
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
