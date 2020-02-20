// The following flags need to be set in cmake:
// -DARROW_CSV=ON
// -DARROW_FILESYSTEM=ON

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/csv/api.h>
#include <arrow/filesystem/api.h>
#include <gflags/gflags.h>
#include <iostream>

DEFINE_string(input_file, "", "The input file containing data to be sorted.");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::cout << "Name of input file: " << FLAGS_input_file << std::endl;
  
  arrow::fs::LocalFileSystem lfs;
  std::shared_ptr<arrow::io::InputStream> input =
      lfs.OpenInputStream(FLAGS_input_file).ValueOrDie();

  arrow::csv::ReadOptions read_options;
  read_options.column_names = {"group_id", "seq_number", "data"};
  arrow::csv::ParseOptions parse_options;
  parse_options.delimiter = '\t';
  arrow::csv::ConvertOptions convert_options;

  std::shared_ptr<arrow::csv::TableReader> table_reader;

  table_reader =
      arrow::csv::TableReader::Make(arrow::default_memory_pool(), input, read_options,
                                    parse_options, convert_options)
          .ValueOrDie();

  std::shared_ptr<arrow::Table> table = table_reader->Read().ValueOrDie();
  std::cout << "table->num_columns: " << table->num_columns() << std::endl;
  std::cout << "table->num_rows: " << table->num_rows() << std::endl;
  std::cout << "table->schema()->ToString(): \n"
            << table->schema()->ToString() << std::endl;

  return 0;
}