#include "common.h"

DEFINE_int32(receiver_port, 32108, "Port the receiver listens on");
DEFINE_bool(debug_mode, false, "If on, more info will be put to stdout");

typedef std::map<std::string, std::vector<std::string>> DestChromo;

class FlightReceiver : public arrow::flight::FlightServerBase {
   public:
    void CreateLogFile() {
        log_file_.open(boost::asio::ip::host_name() + "_flight_receiver.log",
                       std::ios_base::app);
    }

    void ReadChromoDest() {
        DestChromo dest_chromo;
        std::ifstream in_file("chromo_destination.txt");

        std::string chromo_dest_entry;
        std::vector<std::string> chromo_dest_vec;
        std::string delimiters(":");

        while (in_file >> chromo_dest_entry) {
            boost::split(chromo_dest_vec, chromo_dest_entry,
                         boost::is_any_of(delimiters));
            dest_chromo[chromo_dest_vec.at(1)].push_back(chromo_dest_vec.at(0));
        }

        num_of_nodes_ = dest_chromo.size();
        num_plasma_obj_to_receive_ =
            num_of_nodes_ * dest_chromo[boost::asio::ip::host_name()].size();

        log_file_ << PrettyPrintCurrentTime()
                  << "Number of nodes: " << num_of_nodes_ << std::endl;
        log_file_ << PrettyPrintCurrentTime()
                  << "Number of Plasma Objects to receive: "
                  << num_plasma_obj_to_receive_ << std::endl;
    }

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
         * multiple smaller ones onthe receiver's side.
         * Reference: https://stackoverflow.com/a/3692961/5723556
         */
        assert(received_record_batches.size() == 1 &&
               "There is supposed to be only one Record Batch per stream.");

        for (auto record_batch : received_record_batches) {
            auto object_id = PutRecordBatchToPlasma(record_batch);
            object_ids_.push_back(object_id);
            std::cout << "An object has been put to Plasma with ID: "
                      << object_id.binary() << std::endl;
        }

        // There are 25 objects put by BWA into Plasma
        if (GetNumObjInPlasma() == num_plasma_obj_to_receive_ + 25) {
            ProcessReceivedData();
        }
        return arrow::Status::OK();
    }

    std::size_t GetNumObjInPlasma() {
        plasma::PlasmaClient plasma_client;
        ARROW_CHECK_OK(plasma_client.Connect("/tmp/plasma"));
        plasma::ObjectTable objects;
        arrow::Status status = plasma_client.List(&objects);
        std::cout << "Number of objects in the Plamsa Store: " << objects.size()
                  << std::endl;
        return objects.size();
    }

    void ProcessReceivedData() {
        std::ofstream received_objects;
        received_objects.open(
            boost::asio::ip::host_name() + "_id_of_received_objects.log",
            std::ios_base::app);
        for (auto object_id : object_ids_) {
            received_objects << object_id.binary() << std::endl;
        }

        // TODO: start the program for retrieving and sorting here

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

    std::vector<plasma::ObjectID> object_ids_;
    std::ofstream log_file_;
    int num_of_nodes_;
    int num_plasma_obj_to_receive_;
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
    flight_receiver->ReadChromoDest();
    std::cout << "Flight Receiver running on " << boost::asio::ip::host_name()
              << std::endl;
    ARROW_CHECK_OK(flight_receiver->Serve());
    return 0;
}
