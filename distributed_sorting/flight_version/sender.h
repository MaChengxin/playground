#ifndef SENDER_H
#define SENDER_H

#include <list>

#include "common.h"

arrow::Status SendToDestinationNode(std::string host, int port,
                                    plasma::ObjectID object_id);
std::vector<std::string> SeparateServerHosts(std::string server_hosts);

#endif  // SENDER_H
