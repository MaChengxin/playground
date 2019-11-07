#include <iostream>

#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/util/logging.h>
#include <plasma/client.h>

int main(int argc, char **argv)
{
    // Start up and connect a Plasma client.
    plasma::PlasmaClient client;
    ARROW_CHECK_OK(client.Connect("/tmp/store"));
    plasma::ObjectID object_id = plasma::ObjectID::from_binary("0FF1CE00C0FFEE00BEEF");
    plasma::ObjectBuffer object_buffer;
    ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

    // Retrieve object data.
    auto buffer = object_buffer.data;

    arrow::io::BufferReader buffer_reader(buffer); // BufferReader is a derived class of arrow::io::InputStream
    std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
    ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader, &record_batch_stream_reader));

    std::shared_ptr<arrow::RecordBatch> record_batch;
    record_batch_stream_reader->ReadNext(&record_batch);

    if (record_batch->num_columns() == 10 && record_batch->num_rows() == 2000)
    {
        std::cout << "Good, the size of the record batch is correct!" << std::endl;
    }
    else
    {
        std::cout << "Size of the record batch is INCORRECT!" << std::endl;
    }

    const std::vector<std::string> SAM_FIELDS = {"QNAME", "FLAG", "RNAME", "POS", "MAPQ",
                                                 "CIGAR", "RNEXT", "PNEXT", "TLEN", "SEQ"};
    for (int i = 0; i < 10; i++)
    {
        if (record_batch->column_name(i).compare(SAM_FIELDS[i]) == 0)
        {
            std::cout << "Good, " << record_batch->column_name(i) << " is a valid SAM field." << std::endl;
        }
        else
        {
            std::cout << "An invalid SAM field!!! "
                      << "Index: " << i << std::endl;
        }
    }

    // Disconnect the client.
    ARROW_CHECK_OK(client.Disconnect());
}
