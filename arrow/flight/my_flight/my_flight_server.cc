// #include "arrow/api.h"
// #include "arrow/ipc/api.h"

#include <iostream>
#include <memory>

#include <gflags/gflags.h>

#include "arrow/flight/api.h"
// #include "arrow/util/logging.h"

DEFINE_string(server_host, "localhost", "Host where the server is running on");
DEFINE_int32(server_port, 30103, "Server port to listen on");

class MyFlightServer : public arrow::flight::FlightServerBase
{
    arrow::Status DoPut(const arrow::flight::ServerCallContext &context,
                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override
    {
        arrow::flight::FlightStreamChunk chunk;
        while (true)
        {
            RETURN_NOT_OK(reader->Next(&chunk));
            if (!chunk.data)
                break;
            if (chunk.app_metadata)
            {
                RETURN_NOT_OK(writer->WriteMetadata(*chunk.app_metadata));
            }
        }
        return arrow::Status::OK();
    }
};

int main(int argc, char **argv)
{
    std::unique_ptr<MyFlightServer> my_flight_server;
    my_flight_server.reset(new MyFlightServer);

    // arrow::flight::Location location;
    // ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_server_port, &location));
    // arrow::flight::FlightServerOptions options(location);

    // ARROW_CHECK_OK(my_flight_server->Init(options));
    // // Exit with a clean error code (0) on SIGTERM
    // ARROW_CHECK_OK(my_flight_server->SetShutdownOnSignals({SIGTERM}));
    // std::cout << "Server host: " << FLAGS_server_host << std::endl;
    // std::cout << "Server port: " << FLAGS_server_port << std::endl;
    // ARROW_CHECK_OK(my_flight_server->Serve());
    return 0;
}