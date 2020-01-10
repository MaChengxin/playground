#include <gflags/gflags.h>

#include "common.h"
#include "in_memory_storage.h"

DEFINE_string(server_host, "localhost", "Host where the server is running on");
DEFINE_int32(server_port, 30103, "Server port to listen on");
DEFINE_bool(debug_mode, false, "If on, more info will be put to stdout");

class MyFlightServer : public arrow::flight::FlightServerBase {
  arrow::Status DoPut(
      const arrow::flight::ServerCallContext& context,
      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
      std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {
    arrow::flight::FlightStreamChunk chunk;
    while (true) {
      // Question: what is the capacity of a chunk?
      RETURN_NOT_OK(reader->Next(&chunk));
      if (!chunk.data) break;
      received_record_batches.push_back(chunk.data);
      if (chunk.app_metadata) {
        std::cout << "chunk.app_metadata" << chunk.app_metadata->ToString() << std::endl;
        RETURN_NOT_OK(writer->WriteMetadata(*chunk.app_metadata));
      }
    }

    do_put_counter += 1;
    std::cout << "Number of times DoPut has been called for: " << do_put_counter
              << std::endl;

    if (do_put_counter == 3) {
      std::vector<plasma::ObjectID> object_ids;
      for (auto record_batch : received_record_batches) {
        auto object_id = PutRecordBatchToPlasma(record_batch);
        std::cout << "Object ID: " << object_id.hex() << std::endl;
        object_ids.push_back(object_id);
      }

      if (FLAGS_debug_mode) {
        std::cout << "Check if the Record Batches are complete: " << std::endl;

        // Start up and connect a Plasma client
        plasma::PlasmaClient client;
        ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

        for (auto object_id : object_ids) {
          std::shared_ptr<arrow::RecordBatch> record_batch;
          record_batch = GetRecordBatchFromPlasma(object_id, client);
          std::cout << "record_batch->column(2)->ToString()"
                    << record_batch->column(2)->ToString() << std::endl;
        }

        // Disconnect the client
        ARROW_CHECK_OK(client.Disconnect());
      }
    }

    return arrow::Status::OK();
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> received_record_batches;
  int do_put_counter = 0;
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
