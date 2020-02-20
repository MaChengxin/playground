from pyarrow import csv as pacsv

filename = "test_input.txt"


if __name__ == "__main__":
    read_options = pacsv.ReadOptions(
        column_names=["group_id", "seq_number", "data"])

    parse_options = pacsv.ParseOptions(delimiter="\t")

    table = pacsv.read_csv(filename,
                           read_options=read_options,
                           parse_options=parse_options)
