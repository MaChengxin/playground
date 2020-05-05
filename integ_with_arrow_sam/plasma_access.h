#ifndef PLASMA_ACCESS_H
#define PLASMA_ACCESS_H

#include "common.h"

arrow::Status PutRecordBatchToPlasma(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    plasma::ObjectID object_id);

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(
    plasma::ObjectID object_id, plasma::PlasmaClient& client);

#endif  // PLASMA_ACCESS_H
