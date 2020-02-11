import argparse
from datetime import datetime
import pandas as pd
import socket

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file",
                    help="The input file containing data to be sorted.")

parser.add_argument("-k", "--key_type",
                    help="Type of key, can be either integer or string")


if __name__ == "__main__":
    args = parser.parse_args()

    log_file = socket.gethostname()+'_sort_key_val_key_as_'+args.key_type+'.log'

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")

    if args.key_type == "str":
        all_records = pd.read_csv(args.input_file,
                                  sep="\t",
                                  names=["key", "val"],
                                  dtype=str)
    else:
        all_records = pd.read_csv(args.input_file,
                                  sep="\t",
                                  names=["key", "val"])

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished reading input file, started sorting the records\n")

    all_records.sort_values(by=['key'], inplace=True)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sorting the records, started writing to csv\n")

    all_records.reset_index(drop=True, inplace=True)
    all_records.to_csv(socket.gethostname()+'_key_val_sorted_key_as_'+args.key_type+'.csv',
                       sep='\t', header=False, index=False)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished writing to csv \n")
