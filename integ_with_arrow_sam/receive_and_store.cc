#include <boost/asio/ip/host_name.hpp>
#include <cstdlib>

#include <gflags/gflags.h>

#include "common.h"
#include "in_memory_storage.h"

// TODO: change this to inferring from nodeslist
DEFINE_int32(num_nodes, 4, "Number of nodes used for the distributed sorting task");
DEFINE_int32(server_port, 32108, "Server port to listen on");
DEFINE_bool(debug_mode, false, "If on, more info will be put to stdout");

class MyFlightServer : public arrow::flight::FlightServerBase {
  arrow::Status DoPut(
      const arrow::flight::ServerCallContext& context,
      std::unique_ptr<arrow::flight::FlightMessageReader> reader,
      std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {
    auto host_name = boost::asio::ip::host_name();
    std::ofstream log_file;
    log_file.open(host_name + "_flight_receiver.log", std::ios_base::app);

    std::vector<std::shared_ptr<arrow::RecordBatch>> received_record_batches;

    arrow::flight::FlightStreamChunk chunk;
    // Put the received chunks together
    while (true) {  // Assume that there might be multiple chunks in one DoPut
      // Question: what is the capacity of a chunk?
      RETURN_NOT_OK(reader->Next(&chunk));
      if (!chunk.data) break;
      received_record_batches.push_back(chunk.data);
      if (chunk.app_metadata) {
        std::cout << "chunk.app_metadata" << chunk.app_metadata->ToString() << std::endl;
        RETURN_NOT_OK(writer->WriteMetadata(*chunk.app_metadata));
      }
    }

    log_file << PrettyPrintCurrentTime() << "received data, started putting to Plasma"
             << std::endl;

    for (auto record_batch : received_record_batches) {
      auto object_id = PutRecordBatchToPlasma(record_batch);
      object_ids_.push_back(object_id);
    }
    log_file << PrettyPrintCurrentTime() << "putting received data to Plasma finished"
             << std::endl;

    // TODO: separate processing received data from DoPut
    // (https://github.com/MaChengxin/playground/issues/2, not a blocking issue)
    
    // DON'T ASSUME THERE IS ONLY ONE PLASMA OBJECT FROM ONE OTHER NODE!
    
    do_put_counter_ += 1;

    if (do_put_counter_ == FLAGS_num_nodes) {
      ProcessReceivedData();
    }

    return arrow::Status::OK();
  }

 private:
  int do_put_counter_ = 0;
  std::vector<plasma::ObjectID> object_ids_;

  void ProcessReceivedData() {
    std::string object_id_strs;

    for (auto object_id : object_ids_) {
      object_id_strs = object_id_strs + object_id.hex() + " ";
    }

    // std::string python_cmd = "python3 retrieve_and_sort.py " + object_id_strs;
    // std::system(python_cmd.c_str());

    if (FLAGS_debug_mode) {
      PrintRecordBatchesInPlasma(object_ids_);
    }
  }

  void PrintRecordBatchesInPlasma(std::vector<plasma::ObjectID> object_ids) {
    // Start up and connect a Plasma client
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

    for (auto object_id : object_ids) {
      std::cout << "Object ID: " << object_id.hex() << std::endl;
      std::shared_ptr<arrow::RecordBatch> record_batch;
      record_batch = GetRecordBatchFromPlasma(object_id, client);

      std::cout << "record_batch->schema()->ToString(): "
                << record_batch->schema()->ToString() << std::endl;
      std::cout << "record_batch->num_columns(): "
                << record_batch->num_columns() << std::endl;
      std::cout << "record_batch->num_rows(): "
                << record_batch->num_rows() << std::endl;
      std::cout << "record_batch->column(0)->ToString(): "
                << record_batch->column(0)->ToString() << std::endl;
      std::cout << "record_batch->column(2)->ToString(): "
                << record_batch->column(2)->ToString() << std::endl;
      std::cout << "record_batch->column(3)->ToString(): "
                << record_batch->column(3)->ToString() << std::endl;
    }

    // Disconnect the client
    ARROW_CHECK_OK(client.Disconnect());
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
  arrow::flight::FlightServerOptions options(location);

  ARROW_CHECK_OK(my_flight_server->Init(options));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(my_flight_server->SetShutdownOnSignals({SIGTERM}));
  std::cout << "Server port: " << FLAGS_server_port << std::endl;
  ARROW_CHECK_OK(my_flight_server->Serve());
  return 0;
}
