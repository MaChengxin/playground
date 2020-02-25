import argparse
from datetime import datetime
import pandas as pd
import pyarrow as pa

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file", help="The input file.")

if __name__ == "__main__":
    args = parser.parse_args()
    log_file = "process_"+args.input_file.strip(".txt")+".log"

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
        f.write(
            "finished converting to Arrow RecordBatch, started converting to DataFrame for sorting\n")

    try:
        # zero_copy_only=True,
        df = rb.to_pandas(split_blocks=True, self_destruct=True)
        del rb
        with open(log_file, 'a') as f:
            f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
            f.write("finished converting to DataFrame\n")
    except pa.lib.ArrowInvalid:
        with open(log_file, 'a') as f:
            f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
            f.write("Oops! cannot do zero copy conversion\n")

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started sorting\n")

    df.sort_values(by=["RNAME", "POS"], inplace=True)

    # import pdb; pdb.set_trace()

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished sorting, started converting to Arrow RecordBatch\n")

    rb = pa.RecordBatch.from_pandas(df)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished converting to Arrow RecordBatch\n")
