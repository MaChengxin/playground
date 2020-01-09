#include "sorter.h"

bool CompareGroupName(const std::string& a, const std::string& b) {
  std::string group_str = "GROUP";
  std::string a_substr = a.substr(group_str.length());
  int a_substr_int = std::stoi(a_substr);
  std::string b_substr = b.substr(group_str.length());
  int b_substr_int = std::stoi(b_substr);
  return a_substr_int < b_substr_int;
}

/* Source:
 * https://stackoverflow.com/questions/3574680/sort-based-on-multiple-things-in-c*/
bool CompareRecords(const Record& a, const Record& b) {
  if (CompareGroupName(a.group_name, b.group_name)) return true;
  if (CompareGroupName(b.group_name, a.group_name)) return false;

  // a == b for primary condition, go to secondary condition
  if (a.seq < b.seq) return true;
  if (b.seq < a.seq) return false;

  return false;
}

std::shared_ptr<arrow::RecordBatch> ConvertStructVectorToRecordBatch(
    const std::vector<Record>& records) {
  // Hard-coded for now; can be derived from struct Record
  std::shared_ptr<arrow::Schema> schema = arrow::schema(
      {arrow::field("group_name", arrow::utf8()), arrow::field("seq", arrow::int64()),
       arrow::field("data", arrow::utf8())});

  int64_t records_size = static_cast<int64_t>(records.size());

  std::vector<std::string> group_name_vector;
  int64_t seq_array[records_size];
  std::vector<std::string> data_vector;

  for (int64_t i = 0; i < records_size; ++i) {
    group_name_vector.push_back(records[i].group_name);
    seq_array[i] = records[i].seq;
    data_vector.push_back(records[i].data);
  }

  arrow::StringBuilder group_name_builder;
  arrow::Int64Builder seq_builder;
  arrow::StringBuilder data_builder;
  ABORT_NOT_OK(group_name_builder.AppendValues(group_name_vector));
  ABORT_NOT_OK(seq_builder.AppendValues(seq_array, records_size));
  ABORT_NOT_OK(data_builder.AppendValues(data_vector));

  std::shared_ptr<arrow::StringArray> group_name_col;
  std::shared_ptr<arrow::Int64Array> seq_col;
  std::shared_ptr<arrow::StringArray> data_col;
  ABORT_NOT_OK(group_name_builder.Finish(&group_name_col));
  ABORT_NOT_OK(seq_builder.Finish(&seq_col));
  ABORT_NOT_OK(data_builder.Finish(&data_col));

  std::vector<std::shared_ptr<arrow::Array>> batch_cols;
  batch_cols.push_back(group_name_col);
  batch_cols.push_back(seq_col);
  batch_cols.push_back(data_col);

  std::shared_ptr<arrow::RecordBatch> record_batch =
      arrow::RecordBatch::Make(schema, records_size, batch_cols);

  return record_batch;
}

void PutRecordVectorToPlasmaStore(const std::vector<Record>& records,
                                  plasma::ObjectID object_id) {
  // Convert vector of records to a Record Batch
  std::shared_ptr<arrow::RecordBatch> record_batch =
      ConvertStructVectorToRecordBatch(records);

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

  // Create an object with a fixed ObjectID
  // plasma::ObjectID object_id = plasma::ObjectID::from_binary("0FF1CE00C0FFEE00BEEF");
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
}
