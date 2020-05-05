#include <functional>  // for std::function
#include <random>

#include "common.h"

DEFINE_int32(receiver_port, 32108, "Port the receiver listens on");

std::string get_chr_id(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
    auto rname =
        record_batch->GetColumnByName("RNAME")->Slice(0, 1)->ToString();

    std::vector<std::string> rname_str_tokens;
    std::string delimiters("[]");
    boost::split(rname_str_tokens, rname, boost::is_any_of(delimiters));

    std::string chr_id;
    chr_id = rname_str_tokens.at(1);
    chr_id.erase(remove_if(chr_id.begin(), chr_id.end(), isspace),
                 chr_id.end());
    std::string padded_chr_id = std::string(2 - chr_id.length(), '0') + chr_id;
    return padded_chr_id;
}

/* Code for random ID generation is revised from:
 * https://stackoverflow.com/a/12468109/5723556
 */
typedef std::vector<char> char_array;
char_array charset() {
    return char_array({'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
                       'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                       'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
                       'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
                       'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
                       't', 'u', 'v', 'w', 'x', 'y', 'z'});
};

std::string gen_random_str(size_t length, std::function<char(void)> rand_char) {
    std::string str(length, 0);
    std::generate_n(str.begin(), length, rand_char);
    return str;
}

std::string gen_random_id(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
    const auto ch_set = charset();
    std::default_random_engine rng(std::random_device{}());
    std::uniform_int_distribution<> dist(0, ch_set.size() - 1);
    auto randchar = [ch_set, &dist, &rng]() { return ch_set[dist(rng)]; };

    std::string id_base = "RECEIVEDCHROMO";
    std::string chr_id = get_chr_id(record_batch);
    std::string id_suffix = gen_random_str(4, randchar);
    return id_base + chr_id + id_suffix;
}

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
            std::string random_id = gen_random_id(record_batch);
            plasma::ObjectID object_id =
                plasma::ObjectID::from_binary(random_id);
            RETURN_NOT_OK(PutRecordBatchToPlasma(record_batch, object_id));
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
