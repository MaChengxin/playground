#ifndef IN_MEMORY_STORAGE_H
#define IN_MEMORY_STORAGE_H

#include "common.h"

void PutRecordBatchToPlasmaStore(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                  plasma::ObjectID object_id);

#endif // IN_MEMORY_STORAGE_H