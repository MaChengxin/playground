#include <plasma/client.h>

#include "arrow/util/logging.h"

using namespace plasma;

int main(int argc, char **argv) {
    PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/plasma"));
    ObjectID object_id = ObjectID::from_binary("aaaaaaaaaaaaaaaaaaaa");

    client.Delete(object_id);

    ARROW_CHECK_OK(client.Disconnect());
}