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
parser.add_argument(
    "--map_file", help="The file containing mapping between hostnames and input files.")
parser.add_argument(
    "--host_file", help="The file containing the hostnames of the receivers")


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
    args = parser.parse_args()

    with open(args.host_file) as host_file:
        hosts = host_file.readline()
    hosts = hosts.strip("\n").split(",")

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")

    with open(args.map_file) as map_file:
        mapping = map_file.readlines()
        mapping = dict(m.strip("\n").split(":") for m in mapping)

    records = pd.read_csv(mapping[socket.gethostname().split(".")[0]],
                          sep="\t",
                          names=["key", "data"])

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished reading input file, started partitioning the records\n")

    # Reference: https://stackoverflow.com/questions/21441259/pandas-groupby-range-of-values
    # gb = records.groupby(pd.cut(records["key"], len(hosts)))
    # Sometimes we need to specify the bins explicitly to get the correct result
    gb = records.groupby(pd.cut(records["key"],
                                # [0, 120720719+1, 241441439+1, 370720719+1, 491441439+1, 620720719+1, 741441439+1, 870720719+1, 991441439+1]
                                [0, 160960959+1, 330480479+1, 491441439+1, 660960959+1, 830480479+1, 991441439+1],
                                # [0, 491441439+1, 991441439+1],
                                right=False))
    partitioned_records = [gb.get_group(g) for g in gb.groups]

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished partitioning the records, started putting them to Plasma\n")

    # Store the partitioned records to Plasma, to be retrived by C++: DataFrame -> RecordBatch -> Plasma Object
    client = plasma.connect('/tmp/plasma')
    object_ids = [put_df_to_plasma(records, client)
                  for records in partitioned_records]

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished putting partitioned records to Plasma\n")

    with open(socket.gethostname()+'_object_ids.txt', 'w') as f:
        for object_id in object_ids:
            # [9,-1) is to remove prefix and suffix "ObjectID(" and ")"
            f.write(str(object_id)[9:-1]+"\n")

    with open(args.host_file) as host_file:
        hosts = host_file.readline()
    hosts = hosts.strip("\n")

    cpp_proc = ["./send-to-dest", "-server_hosts"]
    cpp_proc.append(hosts)
    subprocess.call(cpp_proc)
