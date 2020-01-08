#include <iostream>

#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/util/logging.h>
#include <plasma/client.h>

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(plasma::ObjectID object_id)
{
    // Start up and connect a Plasma client.
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/store"));

    plasma::ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

    // Retrieve object data.
    auto buffer = object_buffer.data;

    arrow::io::BufferReader buffer_reader(buffer);
    std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
    ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader, &record_batch_stream_reader));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::Status status = record_batch_stream_reader->ReadNext(&record_batch);

    // Disconnect the client.
    ARROW_CHECK_OK(client.Disconnect());

    return record_batch;
}

int main(int argc, char **argv)
{
    plasma::ObjectID object_id = plasma::ObjectID::from_binary("0FF1CE00C0FFEE00BEEF");

    // Start up and connect a Plasma client.
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/store"));

    plasma::ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

    // Retrieve object data.
    auto buffer = object_buffer.data;

    arrow::io::BufferReader buffer_reader(buffer); // BufferReader is a derived class of arrow::io::InputStream
    std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
    ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader, &record_batch_stream_reader));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::Status status = record_batch_stream_reader->ReadNext(&record_batch);

    // std::shared_ptr<arrow::RecordBatch> record_batch = GetRecordBatchFromPlasma(object_id);

    std::cout << "record_batch->column_name(0): " << record_batch->column_name(0) << std::endl;
    std::cout << "record_batch->num_columns(): " << record_batch->num_columns() << std::endl;
    std::cout << "record_batch->num_rows(): " << record_batch->num_rows() << std::endl;

    std::cout << "record_batch->column(0)->length(): "
              << record_batch->column(0)->length() << std::endl;
    std::cout << "record_batch->column(0)->ToString(): "
              << record_batch->column(0)->ToString() << std::endl;
}
