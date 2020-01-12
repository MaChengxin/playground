import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
import binascii

# https://arrow.apache.org/docs/python/plasma.html#getting-pandas-dataframes-from-plasma


def get_record_batch_from_plasma(object_id, client):
    [buffer] = client.get_buffers([object_id])
    buffer_reader = pa.BufferReader(buffer)
    reader = pa.RecordBatchStreamReader(buffer_reader)
    record_batch = reader.read_next_batch()

    return record_batch


if __name__ == "__main__":
    hex_strs = ['ac2f75c043fbc36709d315f2245746d8588c3ac1',
                '25eb8c48ff89cb854fc09081cc47edfc8619b214',
                'a80fed48162bd24b6807a2b15f4bd52f3f1fda94']

    bin_strs = [binascii.unhexlify(hex_str) for hex_str in hex_strs]

    object_ids = [plasma.ObjectID(bin_str) for bin_str in bin_strs]

    client = plasma.connect("/tmp/plasma")

    record_batches = [get_record_batch_from_plasma(
        object_id, client) for object_id in object_ids]

    dfs = [record_batch.to_pandas() for record_batch in record_batches]
    all_records = pd.concat(dfs)
    all_records.sort_values(by=['group_name', 'seq'], inplace=True)
