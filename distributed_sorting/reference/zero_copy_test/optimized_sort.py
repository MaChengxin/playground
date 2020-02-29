# This is for reference only.
# Performance isn't improved.

import argparse
from datetime import datetime
import pandas as pd
import pyarrow as pa

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file", help="The input file.")

if __name__ == "__main__":
    args = parser.parse_args()
    log_file = "opti_process_"+args.input_file.strip(".txt")+".log"

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file "+args.input_file+"\n")

    records = pd.read_csv(args.input_file,
                          sep="\t",
                          names=["QNAME", "FLAG", "RNAME",
                                 "POS", "MAPQ", "CIGAR", "RNEXT",
                                 "PNEXT", "TLEN", "SEQ", "QUAL"])

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished reading input file, started converting to Arrow RecordBatch\n")

    rb = pa.RecordBatch.from_pandas(records)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished converting to Arrow RecordBatch, started retrieving columns for sorting\n")

    rname_col = rb.column(2).to_pandas()
    pos_col = rb.column(3).to_pandas()

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished retrieving columns for sorting, concatenating them as df\n")

    df_for_sorting = pd.concat([rname_col, pos_col], axis=1)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished concatenating columns to df, started sorting it\n")

    df_for_sorting.sort_values(by=["RNAME", "POS"], inplace=True)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sorting df, started making new rb\n")

    idx = df_for_sorting.index

    new_rb_cols = []

    # for i in range(11):
    #     orig_col = rb.column(i)
    #     col_as_pandas = orig_col.to_pandas()
    #     new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))       
    #     new_rb_cols.append(new_col)

    # for i in range(11):
    #     orig_col = rb.column(i)
    #     col_as_numpy_array = orig_col.to_numpy()
    #     new_col = pa.array(col_as_numpy_array[idx])
    #     new_rb_cols.append(new_col)
    
    orig_col = rb.column(0) # QNAME, string
    col_as_pandas = orig_col.to_pandas()
    new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending QNAME\n")

    orig_col = rb.column(1) # FLAG, int
    col_as_numpy_array = orig_col.to_numpy()
    new_col = pa.array(col_as_numpy_array[idx])
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending FLAG\n")

    orig_col = rb.column(2) # RNAME, string
    col_as_pandas = orig_col.to_pandas()
    new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending RNAME\n")

    orig_col = rb.column(3) # POS, int
    col_as_numpy_array = orig_col.to_numpy()
    new_col = pa.array(col_as_numpy_array[idx])
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending POS\n")

    orig_col = rb.column(4) # MAPQ, int
    col_as_numpy_array = orig_col.to_numpy()
    new_col = pa.array(col_as_numpy_array[idx])
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending MAPQ\n")

    orig_col = rb.column(5) # CIGAR, string
    col_as_pandas = orig_col.to_pandas()
    new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending CIGAR\n")

    orig_col = rb.column(6) # RNEXT, string
    col_as_pandas = orig_col.to_pandas()
    new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending RNEXT\n")

    orig_col = rb.column(7) # PNEXT, int
    col_as_numpy_array = orig_col.to_numpy()
    new_col = pa.array(col_as_numpy_array[idx])
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending PNEXT\n")

    orig_col = rb.column(8) # TLEN, int
    col_as_numpy_array = orig_col.to_numpy()
    new_col = pa.array(col_as_numpy_array[idx])
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending TLEN\n")

    orig_col = rb.column(9) # SEQ, string
    col_as_pandas = orig_col.to_pandas()
    new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending SEQ\n")

    orig_col = rb.column(10) # QUAL, string
    col_as_pandas = orig_col.to_pandas()
    new_col = pa.Array.from_pandas(col_as_pandas.reindex(idx))
    new_rb_cols.append(new_col)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished appending QUAL\n")

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(">> finished building rb_cols\n")

    new_rb = pa.RecordBatch.from_arrays(new_rb_cols, schema=rb.schema)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished making new rb\n")

    # import pdb; pdb.set_trace()
