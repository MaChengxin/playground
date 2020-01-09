#include <iostream>

#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/util/logging.h>
#include <plasma/client.h>

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(plasma::ObjectID object_id,
                                                             plasma::PlasmaClient &client)
{
    plasma::ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

    auto buffer = object_buffer.data;

    arrow::io::BufferReader buffer_reader(buffer);
    std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
    ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader, &record_batch_stream_reader));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::Status status = record_batch_stream_reader->ReadNext(&record_batch);

    return record_batch;
}

int main(int argc, char **argv)
{
    plasma::ObjectID object_id = plasma::ObjectID::from_binary("0FF1CE00C0FFEE00BEEF");

    // Start up and connect a Plasma client
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/store"));

    std::shared_ptr<arrow::RecordBatch> record_batch = GetRecordBatchFromPlasma(object_id, client);

    // Disconnect the Plasma client
    ARROW_CHECK_OK(client.Disconnect());

    std::cout << "record_batch->column(0)->ToString(): "
              << record_batch->column(0)->ToString() << std::endl;
}
