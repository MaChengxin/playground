#include <arrow/util/thread_pool.h>

#include "common.h"

DEFINE_int32(destination_port, 32108, "Port on the destinations to connect to");
DEFINE_int32(thread_pool_size, 25, "Size of the thread pool for Flight tasks");

// https://stackoverflow.com/a/1842976/5723556
typedef std::map<std::string, std::pair<std::string, std::string>> DispatchPlan;
typedef DispatchPlan::const_iterator DispatchPlanIter;

arrow::Status Takeoff(std::string host, int port, plasma::ObjectID object_id) {
    arrow::Status status;

    // Start up a Plasma client and connection it to Plasma Store
    plasma::PlasmaClient plasma_client;
    ARROW_CHECK_OK(plasma_client.Connect("/tmp/plasma"));
    std::shared_ptr<arrow::RecordBatch> record_batch;
    record_batch = GetRecordBatchFromPlasma(object_id, plasma_client);

    // Set up the Flight
    std::unique_ptr<arrow::flight::FlightClient> flight_client;
    arrow::flight::Location location;
    ABORT_NOT_OK(arrow::flight::Location::ForGrpcTcp(host, port, &location));
    ABORT_NOT_OK(
        arrow::flight::FlightClient::Connect(location, &flight_client));

    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> reader;
    status = flight_client->DoPut(arrow::flight::FlightDescriptor{},
                                  record_batch->schema(), &writer, &reader);
    if (!status.ok()) {
        return status;
    }
    // Write the Record Batch to Flight stream
    status = writer->WriteRecordBatch(*record_batch);
    status = writer->Close();
    if (!status.ok()) {
        return status;
    }

    // Disconnect the Plasma client
    ARROW_CHECK_OK(plasma_client.Disconnect());

    return arrow::Status::OK();
}

arrow::Status TakeoffAll(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Create a log file
    auto host_name = boost::asio::ip::host_name();
    // Reference code: https://stackoverflow.com/a/32435076/5723556
    std::regex pattern(".bullx");  // extension of hostname on Cartesius
    host_name = std::regex_replace(host_name, pattern, "");
    std::ofstream log_file;
    log_file.open(host_name + "_flight_sender.log", std::ios_base::app);
    log_file << PrettyPrintCurrentTime() << "send-to-dest started" << std::endl;

    // Get Plasma Object IDs and associated destinations from the dispatch plan
    std::ifstream in_file(host_name + "_dispatch_plan.txt");
    std::string dispatch_plan_entry;
    // https://stackoverflow.com/a/16889840/5723556
    std::vector<std::string> dispatch_plan_fields;
    std::string delimiters(":,");
    DispatchPlan dispatch_plan;

    while (in_file >> dispatch_plan_entry) {
        boost::split(dispatch_plan_fields, dispatch_plan_entry,
                     boost::is_any_of(delimiters));

        /* position 0: chromo
           position 1: destination
           position 2: Plasma Object ID
        */
        dispatch_plan[dispatch_plan_fields.at(0)] = std::make_pair(
            dispatch_plan_fields.at(1), dispatch_plan_fields.at(2));
    }

    // Make a Thread Pool for the Flight tasks. Reference code:
    // https://github.com/apache/arrow/blob/master/cpp/src/arrow/flight/flight_benchmark.cc
    ARROW_ASSIGN_OR_RAISE(auto pool, arrow::internal::ThreadPool::Make(FLAGS_thread_pool_size));
    std::vector<std::future<Status>> tasks;

    for (DispatchPlanIter iter = dispatch_plan.begin(); iter != dispatch_plan.end(); ++iter) {
        // There is no need to send objects to self
        if (iter->second.first != host_name) {
        log_file << PrettyPrintCurrentTime() << "Scheduling sending "
                 << iter->first << " to " << iter->second.first
                 << ", local Object ID: " << iter->second.second << std::endl;
            ARROW_ASSIGN_OR_RAISE(auto task,
                                 pool->Submit(Takeoff, 
                                              iter->second.first,
                                              FLAGS_destination_port,
                                              plasma::ObjectID::from_binary(iter->second.second)));
            tasks.push_back(std::move(task));
        }
    }

    log_file << PrettyPrintCurrentTime() << "All Flights scheduled. " << std::endl;

    // Wait for tasks to finish
    for (auto &&task : tasks) {
        RETURN_NOT_OK(task.get());
    }

    return arrow::Status::OK();
}

int main(int argc, char **argv) {
    arrow::Status status = TakeoffAll(argc, argv);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
    }
    return 0;
}
