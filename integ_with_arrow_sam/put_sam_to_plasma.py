""" Script for processing data in SAM format.
Useful links:
    https://github.com/apache/arrow/blob/master/python/examples/plasma/sorting/sort_df.py
"""

import pandas as pd
import pyarrow as pa
from pyarrow import plasma

SAM_FIELDS = ["QNAME", "FLAG", "RNAME", "POS", "MAPQ", "CIGAR",
              "RNEXT", "PNEXT", "TLEN", "SEQ", "QUAL", "OPTIONAL"]


def put_df_to_object_store(client, df, object_id):
    """ Precondition: the Plasma Object Store has been opened.
    e.g. by: plasma_store -m 1000000000 -s /tmp/plasma
    Returns the object ID.
    """
    record_batch = pa.RecordBatch.from_pandas(df)
    # Get size of record batch and schema
    mock_output_stream = pa.MockOutputStream()
    stream_writer = pa.RecordBatchStreamWriter(mock_output_stream,
                                               record_batch.schema)
    stream_writer.write_batch(record_batch)
    data_size = mock_output_stream.size()

    # Allocate a buffer in the object store for the serialized DataFrame
    buf = client.create(object_id, data_size)

    # Write the serialized DataFrame to the object store
    stream = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)

    print("Finished putting a DataFrame to Plasma. Object ID: " +
          object_id.binary().decode())


def generate_object_id(offset):
    id_base = b"0FF1CEBEEFC0FFEE"
    encocded_offset = str(offset).zfill(4).encode("ASCII")
    object_id = plasma.ObjectID(id_base+encocded_offset)
    return object_id


if __name__ == "__main__":
    with open("2k_reads.sam") as f:
        lines = f.readlines()

    split_lines = []
    for line in lines:
        split_line = line.split("\t", len(SAM_FIELDS)-1)
        split_line[-1] = split_line[-1].strip("\n")
        split_lines.append(split_line)

    df = pd.DataFrame.from_records(split_lines, columns=SAM_FIELDS)

    df = df.astype({"FLAG": "int64", "POS": "int64", "MAPQ": "int64",
                    "PNEXT": "int64", "TLEN": "int64"})

    client = plasma.connect("/tmp/plasma")

    gb = df.groupby("RNAME")

    for i, g in enumerate(gb.groups):
        put_df_to_object_store(client, gb.get_group(g), generate_object_id(i))
