#include "plasma_access.h"

#include <functional>  // for std::function
#include <random>

/* Code for random ID generation is revised from:
 * https://stackoverflow.com/a/12468109/5723556
 */
typedef std::vector<char> char_array;
char_array charset() {
    return char_array({'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A',
                       'B', 'C', 'D', 'E', 'F'});
};

std::string gen_random_str(size_t length, std::function<char(void)> rand_char) {
    std::string str(length, 0);
    std::generate_n(str.begin(), length, rand_char);
    return str;
}

std::string gen_random_id() {
    const auto ch_set = charset();
    std::default_random_engine rng(std::random_device{}());
    std::uniform_int_distribution<> dist(0, ch_set.size() - 1);
    auto randchar = [ch_set, &dist, &rng]() { return ch_set[dist(rng)]; };
    return gen_random_str(20, randchar);
}

plasma::ObjectID PutRecordBatchToPlasma(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  // Get size of the Record Batch
  arrow::io::MockOutputStream mock_output_stream;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
  arrow::Status status;
  status = arrow::ipc::RecordBatchStreamWriter::Open(
      &mock_output_stream, record_batch->schema(), &record_batch_writer);
  if (!status.ok()) {
    std::cerr << "arrow::ipc::RecordBatchStreamWriter::Open failed with error: "
              << status.ToString() << std::endl;
  }
  status = record_batch_writer->WriteRecordBatch(*record_batch);
  if (!status.ok()) {
    std::cerr << "record_batch_writer->WriteRecordBatch failed with error: "
              << status.ToString() << std::endl;
  }
  int64_t data_size = mock_output_stream.GetExtentBytesWritten();

  // Start up and connect a Plasma client
  plasma::PlasmaClient client;
  ARROW_CHECK_OK(client.Connect("/tmp/plasma"));

  // Create an object with a random Object ID
  std::string random_id = gen_random_id();
  plasma::ObjectID object_id =plasma::ObjectID::from_binary(random_id);
  std::shared_ptr<Buffer> buf;
  ARROW_CHECK_OK(client.Create(object_id, data_size, nullptr, 0, &buf));

  // Write the Record Batch into the object
  arrow::io::FixedSizeBufferWriter buffer_writer(buf);
  status = arrow::ipc::RecordBatchStreamWriter::Open(
      &buffer_writer, record_batch->schema(), &record_batch_writer);
  if (!status.ok()) {
    std::cerr << "arrow::ipc::RecordBatchStreamWriter::Open failed with error: "
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

  return object_id;
}

std::shared_ptr<arrow::RecordBatch> GetRecordBatchFromPlasma(
    plasma::ObjectID object_id, plasma::PlasmaClient& client) {
  plasma::ObjectBuffer object_buffer;
  ARROW_CHECK_OK(client.Get(&object_id, 1, -1, &object_buffer));

  auto buffer = object_buffer.data;

  arrow::io::BufferReader buffer_reader(buffer);
  std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_stream_reader;
  ARROW_CHECK_OK(arrow::ipc::RecordBatchStreamReader::Open(&buffer_reader,
                                                           &record_batch_stream_reader));

  std::shared_ptr<arrow::RecordBatch> record_batch;
  arrow::Status status = record_batch_stream_reader->ReadNext(&record_batch);

  return record_batch;
}
