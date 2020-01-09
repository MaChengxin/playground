#ifndef SORTER_H
#define SORTER_H

#include "common.h"

bool CompareRecords(const Record& a, const Record& b);
std::shared_ptr<arrow::RecordBatch> ConvertStructVectorToRecordBatch(
    const std::vector<Record>& records);

#endif // SORTER_H
