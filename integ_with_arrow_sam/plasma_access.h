#ifndef PLASMA_ACCESS_H
#define PLASMA_ACCESS_H

#include "common.h"

plasma::ObjectID PutRecordBatchToPlasma(
    const std::shared_ptr<arrow::RecordBatch>& record_batch);

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(
    plasma::ObjectID object_id, plasma::PlasmaClient& client);

#endif  // PLASMA_ACCESS_H
