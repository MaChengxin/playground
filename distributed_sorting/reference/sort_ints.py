import argparse
from datetime import datetime
import pandas as pd
import socket

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file",
                    help="The input file containing data to be sorted.")


if __name__ == "__main__":
    args = parser.parse_args()

    with open(socket.gethostname()+'_sort_ints.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")

    all_records = pd.read_csv(args.input_file,
                              sep="\t",
                              names=["ints"])

    with open(socket.gethostname()+'_sort_ints.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished reading input file, started sorting the records\n")

    all_records.sort_values(by=['ints'], inplace=True)

    with open(socket.gethostname()+'_sort_ints.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sorting the records, started writing to csv\n")

    all_records.reset_index(drop=True, inplace=True)
    all_records.to_csv(socket.gethostname()+'_ints_sorted.csv',
                       sep='\t', header=False, index=False)

    with open(socket.gethostname()+'_sort_ints.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished writing to csv \n")
