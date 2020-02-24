import argparse
from datetime import datetime
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file",
                    help="The input file containing data to be sorted.")

if __name__ == "__main__":
    args = parser.parse_args()

    log_file = 'sort_records_by_python.log'

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")

    all_records = pd.read_csv(args.input_file,
                              sep="\t",
                              names=["f0", "f1", "f2",
                                     "f3", "f4", "f5",
                                     "f6", "f7", "f8", "f9"])

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished reading input file, started sorting the records\n")

    all_records.sort_values(by=['f0', 'f1'], inplace=True)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sorting the records, started writing to csv\n")

    all_records.reset_index(drop=True, inplace=True)
    all_records.to_csv('records_sorted_by_python.csv',
                       sep='\t', header=False, index=False)

    with open(log_file, 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished writing to csv \n")
