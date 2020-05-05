#include "plasma_access.h"

arrow::Status PutRecordBatchToPlasma(
    const std::shared_ptr<arrow::RecordBatch>& record_batch,
    plasma::ObjectID object_id) {
    // Get size of the Record Batch
    arrow::io::MockOutputStream mock_output_stream;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
    arrow::Status status;
    status = arrow::ipc::RecordBatchStreamWriter::Open(
        &mock_output_stream, record_batch->schema(), &record_batch_writer);
    if (!status.ok()) {
        std::cerr
            << "arrow::ipc::RecordBatchStreamWriter::Open failed with error: "
            << status.ToString() << std::endl;
    }
    status = record_batch_writer->WriteRecordBatch(*record_batch);
    if (!status.ok()) {
        std::cerr << "record_batch_writer->WriteRecordBatch failed with error: "
                  << status.ToString() << std::endl;
    }
    int64_t data_size = mock_output_stream.GetExtentBytesWritten();

    // Start up a client and connect it to Plasma
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

    // Create an object with the given Object ID
    std::shared_ptr<Buffer> buf;
    ARROW_CHECK_OK(client.Create(object_id, data_size, nullptr, 0, &buf));

    // Write the Record Batch into the object
    arrow::io::FixedSizeBufferWriter buffer_writer(buf);
    status = arrow::ipc::RecordBatchStreamWriter::Open(
        &buffer_writer, record_batch->schema(), &record_batch_writer);
    if (!status.ok()) {
        std::cerr
            << "arrow::ipc::RecordBatchStreamWriter::Open failed with error: "
            << status.ToString() << std::endl;
    }
    status = record_batch_writer->WriteRecordBatch(*record_batch);
    if (!status.ok()) {
        std::cerr << "record_batch_writer->WriteRecordBatch failed with error: "
                  << status.ToString() << std::endl;
    }

    // Seal the object
    ARROW_CHECK_OK(client.Seal(object_id));

    // Disconnect the client
    ARROW_CHECK_OK(client.Disconnect());

    return status;
}

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(
    plasma::ObjectID object_id, plasma::PlasmaClient& client) {
    plasma::ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

    auto buffer = object_buffer.data;

    arrow::io::BufferReader buffer_reader(buffer);
    std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
    ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(
        &buffer_reader, &record_batch_stream_reader));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    arrow::Status status = record_batch_stream_reader->ReadNext(&record_batch);

    return record_batch;
}
