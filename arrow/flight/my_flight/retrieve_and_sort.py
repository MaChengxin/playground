""" This file needs to be put with the receive-and-store executable in the same directory.
"""

import binascii
from datetime import datetime
import socket
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma


def get_record_batch_from_plasma(object_id, client):
    """ https://arrow.apache.org/docs/python/plasma.html#getting-pandas-dataframes-from-plasma
    """
    [buffer] = client.get_buffers([object_id])
    buffer_reader = pa.BufferReader(buffer)
    reader = pa.RecordBatchStreamReader(buffer_reader)
    record_batch = reader.read_next_batch()

    return record_batch


if __name__ == "__main__":
    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("retrieve_and_sort.py started\n")
    hex_strs = sys.argv[1:]
    object_ids = [plasma.ObjectID(binascii.unhexlify(hex_str))
                  for hex_str in hex_strs]

    client = plasma.connect("/tmp/plasma")

    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started retrieving and assembling the records\n")
    dfs = [get_record_batch_from_plasma(object_id, client).to_pandas()
           for object_id in object_ids]

    all_records = pd.concat(dfs)
    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished retrieving and assembling the records, started sorting the records\n")

    all_records.sort_values(by=['group_name', 'seq'], inplace=True)
    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sorting the records, started writing to csv\n")
    all_records.reset_index(drop=True, inplace=True)
    all_records.to_csv(socket.gethostname()+'.csv', sep='\t', index=False)

    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("retrieve_and_sort.py finished\n")
