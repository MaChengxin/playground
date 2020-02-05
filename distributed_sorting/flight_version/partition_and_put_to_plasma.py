""" This file needs to be put with the send-to-dest executable in the same directory.
"""

import argparse
from datetime import datetime
import socket
import subprocess

import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file",
                    help="The input file containing data to be sorted.")
parser.add_argument("--hosts", help="The receivers")
parser.add_argument("-p", "--partition_boundaries",
                    help="The boundaries to partition the records.")


def put_df_to_plasma(df, client):
    """ Precondition: the Plasma Object Store has been opened.
        Returns the object ID.
    """
    record_batch = pa.RecordBatch.from_pandas(df)

    # Get size of record batch and schema
    mock_output_stream = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_output_stream,
                                               record_batch.schema)
    stream_writer.write_batch(record_batch)
    data_size = mock_output_stream.size()

    object_id = plasma.ObjectID.from_random()

    buf = client.create(object_id, data_size)
    stream = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)

    return object_id


if __name__ == "__main__":
    # Parse the arguments
    args = parser.parse_args()
    pb = args.partition_boundaries.split(',')
    pb = list(map(int, pb))
    partitioned_groups = []

    def gen_groups(low, high):
        return ['GROUP'+str(i) for i in range(low, high)]
    for i in range(len(pb)-1):
        partitioned_groups.append(gen_groups(pb[i], pb[i+1]))

    """ partitioned_groups will look something like this given -p 0,3,6,10:
    [['GROUP0', 'GROUP1', 'GROUP2'], ['GROUP3', 'GROUP4', 'GROUP5'], ['GROUP6', 'GROUP7', 'GROUP8', 'GROUP9']]
    """

    # Read the input file, construct the data to be partitioned: csv/txt -> DataFrame
    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")
    records = pd.read_csv(args.input_file,
                          sep="\t",
                          names=["group_name", "seq", "data"])
    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished reading input file, started partitioning the records, started groupby()\n")

    # Partition the records: DataFrame -> DataFrame
    # https://stackoverflow.com/questions/47769453/pandas-split-dataframe-to-multiple-by-unique-values-rows
    dfs = dict(tuple(records.groupby('group_name')))
    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            ">> partitioning the records, groupby() finished, pd.concat() started \n")
    sub_records = []
    for i in range(len(partitioned_groups)):
        sub_records.append(pd.concat([dfs[group]
                                      for group in partitioned_groups[i]]))
    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "pd.concat() finished, finished partitioning the records, started putting them to Plasma\n")

    # Store the partitioned records to Plasma, to be retrived by C++: DataFrame -> RecordBatch -> Plasma Object
    client = plasma.connect('/tmp/plasma')
    object_ids = [put_df_to_plasma(records, client) for records in sub_records]

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished putting partitioned records to Plasma\n")

    with open(socket.gethostname()+'_object_ids.txt', 'w') as f:
        for object_id in object_ids:
            # [9,-1) is to remove prefix and suffix "ObjectID(" and ")"
            f.write(str(object_id)[9:-1]+"\n")
    
    cpp_proc = ["./send-to-dest", "-server_hosts"]
    cpp_proc.append(args.hosts)
    subprocess.call(cpp_proc)
