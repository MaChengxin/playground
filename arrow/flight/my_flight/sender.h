#ifndef SENDER_H
#define SENDER_H

#include <list>

#include "common.h"

arrow::Status SendToResponsibleNode(std::string host, int port,
                                    const arrow::RecordBatch& record_batch);
std::vector<std::string> SeparateServerHosts(std::string server_hosts);
std::list<std::string> SeparatePartitionBoundaries(std::string boundaries);

#endif  // SENDER_H
