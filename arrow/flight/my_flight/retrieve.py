import binascii
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
    hex_strs = sys.argv[1:]
    object_ids = [plasma.ObjectID(binascii.unhexlify(hex_str))
                  for hex_str in hex_strs]

    client = plasma.connect("/tmp/plasma")

    dfs = [get_record_batch_from_plasma(object_id, client).to_pandas()
           for object_id in object_ids]

    all_records = pd.concat(dfs)
    all_records.sort_values(by=['group_name', 'seq'], inplace=True)
    all_records.reset_index(drop=True, inplace=True)
    all_records.to_csv(socket.gethostname()+'.csv', sep='\t', index=False)
