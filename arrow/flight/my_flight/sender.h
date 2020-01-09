#ifndef SENDER_H
#define SENDER_H

#include "common.h"

arrow::Status SendToResponsibleNode(std::string host, int port,
                                    const arrow::RecordBatch& record_batch);
std::vector<std::string> SeparateServerHosts(std::string server_hosts);

#endif // SENDER_H
