#include "arrow/util/logging.h"
#include <plasma/client.h>

using namespace plasma;

int main(int argc, char **argv)
{
    // Start up and connect a Plasma client.
    PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/plasma"));
    ObjectID object_id = ObjectID::from_binary("00000000000000000000");
    ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

    // Retrieve object data.
    auto buffer = object_buffer.data;
    const uint8_t *data = buffer->data();
    int64_t data_size = buffer->size();

    // Check that the data agrees with what was written in the other process.
    for (int64_t i = 0; i < data_size; i++)
    {
        ARROW_CHECK(data[i] == static_cast<uint8_t>(i % 4));
    }

    // Disconnect the client.
    ARROW_CHECK_OK(client.Disconnect());
}