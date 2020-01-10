#ifndef IN_MEMORY_STORAGE_H
#define IN_MEMORY_STORAGE_H

#include "common.h"

plasma::ObjectID PutRecordBatchToPlasma(
    const std::shared_ptr<arrow::RecordBatch>& record_batch);

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(
    plasma::ObjectID object_id, plasma::PlasmaClient& client);

#endif  // IN_MEMORY_STORAGE_H