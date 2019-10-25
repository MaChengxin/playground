#include "arrow/util/logging.h"
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char **argv)
{
    // Start up and connect a Plasma client.
    PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

    // Create an object with a fixed ObjectID.
    ObjectID object_id = ObjectID::from_binary("00000000000000000000");
    int64_t data_size = 1000;
    std::shared_ptr<Buffer> data;
    std::string metadata = "{'author': 'john'}";
    ARROW_CHECK_OK(client.Create(object_id, data_size, (uint8_t *)metadata.data(), metadata.size(), &data));

    // Write some data into the object.
    auto d = data->mutable_data();
    for (int64_t i = 0; i < data_size; i++)
    {
        d[i] = static_cast<uint8_t>(i % 4);
    }

    // Seal the object.
    ARROW_CHECK_OK(client.Seal(object_id));

    // Disconnect the client.
    ARROW_CHECK_OK(client.Disconnect());
}