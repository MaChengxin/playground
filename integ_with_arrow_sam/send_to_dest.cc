#include <map>

#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <gflags/gflags.h>

#include <arrow/util/thread_pool.h>

#include "common.h"
#include "sender.h"

DEFINE_int32(server_port, 30103, "The port to connect to");

// https://stackoverflow.com/a/1842976/5723556
typedef std::map<std::string, std::pair<std::string, plasma::ObjectID>> DispatchPlan;
typedef DispatchPlan::const_iterator DispatchPlanIter;

arrow::Status SendToDest(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto host_name = boost::asio::ip::host_name();
    std::ofstream log_file;
    log_file.open(host_name + "_s.log", std::ios_base::app);
    log_file << PrettyPrintCurrentTime() << "send-to-dest started" << std::endl;

    // Get the Object IDs and their destinations
    std::vector<plasma::ObjectID> object_ids;
    std::vector<std::string> destinations;

    std::ifstream in_file(host_name + "_dispatch_plan.txt");
    std::string dispatch_plan_entry;
    // https://stackoverflow.com/a/16889840/5723556
    std::vector<std::string> dispatch_plan_fields;
    std::string delimiters(":,");
    DispatchPlan dispatch_plan;

    while (in_file >> dispatch_plan_entry) {
        boost::split(dispatch_plan_fields, dispatch_plan_entry,
                     boost::is_any_of(delimiters));

        dispatch_plan[dispatch_plan_fields.at(0)] = std::make_pair(
            dispatch_plan_fields.at(1),
            plasma::ObjectID::from_binary(dispatch_plan_fields.at(2)));
    }


    // Reference code:
    // https://github.com/apache/arrow/blob/master/cpp/src/arrow/flight/flight_benchmark.cc
    ARROW_ASSIGN_OR_RAISE(auto pool, arrow::internal::ThreadPool::Make(4));
    std::vector<std::future<Status>> tasks;

        for (DispatchPlanIter iter(dispatch_plan.begin()); iter != dispatch_plan.end(); ++iter) {
        std::cout << iter->first << iter->second.first << '\n';
              ARROW_ASSIGN_OR_RAISE(auto task,
                                     pool->Submit(SendToDestinationNode,
                                                   iter->second.first,
                                                    FLAGS_server_port,
                                                    iter->second.second));
      tasks.push_back(std::move(task));
    }

    // Wait for tasks to finish
    for (auto&& task : tasks) {
      RETURN_NOT_OK(task.get());
    }

    return arrow::Status::OK();
}

int main(int argc, char **argv) {
    arrow::Status status = SendToDest(argc, argv);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
    }
    return 0;
}
