#include <iostream>
#include "arrow/util/logging.h"
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char **argv)
{
    // Start up and connect a Plasma client.
    PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

    int fd;
    ARROW_CHECK_OK(client.Subscribe(&fd));

    ObjectID object_id;
    int64_t data_size;
    int64_t metadata_size;
    while (data_size == 0)
    {
        ARROW_CHECK_OK(client.GetNotification(fd, &object_id, &data_size, &metadata_size));

        std::cout << "Received object notification for object_id = "
                  << object_id.hex() << ", with data_size = " << data_size
                  << ", and metadata_size = " << metadata_size << std::endl;
    }
}