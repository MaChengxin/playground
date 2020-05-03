#include "common.h"

DEFINE_int32(receiver_port, 32108, "Port the receiver listens on");

class FlightReceiver : public arrow::flight::FlightServerBase {
   public:
    void CreateLogFile() {
        host_name = boost::asio::ip::host_name();
        std::regex pattern(".bullx");  // extension of hostname on Cartesius
        host_name = std::regex_replace(host_name, pattern, "");
        log_file_.open(host_name + "_flight_receiver.log", std::ios_base::app);
    }

    std::string host_name;

   private:
    arrow::Status DoPut(
        const arrow::flight::ServerCallContext& context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override {
        std::vector<std::shared_ptr<arrow::RecordBatch>>
            received_record_batches;

        RETURN_NOT_OK(reader->ReadAll(&received_record_batches));

        /* In our case we only write one Record Batch per stream. Assume that
         * Flight doesn't break a Record Batch from the sender's side into
         * multiple smaller ones on the receiver's side.
         * Reference: https://stackoverflow.com/a/3692961/5723556
         */
        assert(received_record_batches.size() == 1 &&
               "There is supposed to be only one Record Batch per stream.");

        for (auto record_batch : received_record_batches) {
            auto object_id = PutRecordBatchToPlasma(record_batch);
            log_file_ << PrettyPrintCurrentTime()
                      << "Flight Receiver has put an object to Plasma with "
                         "Object ID: "
                      << object_id.binary() << std::endl;
        }

        return arrow::Status::OK();
    }

    std::ofstream log_file_;
};

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::unique_ptr<FlightReceiver> flight_receiver;
    flight_receiver.reset(new FlightReceiver);

    arrow::flight::Location location;
    ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp(
        "0.0.0.0", FLAGS_receiver_port, &location));
    arrow::flight::FlightServerOptions options(location);

    ARROW_CHECK_OK(flight_receiver->Init(options));
    // Exit with a clean error code (0) on SIGTERM
    ARROW_CHECK_OK(flight_receiver->SetShutdownOnSignals({SIGTERM}));
    flight_receiver->CreateLogFile();
    std::cout << "Flight Receiver running on " << flight_receiver->host_name
              << std::endl;
    ARROW_CHECK_OK(flight_receiver->Serve());
    return 0;
}
