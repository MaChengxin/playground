""" Script for processing data in SAM format.
Useful links:
    https://github.com/apache/arrow/blob/master/python/examples/plasma/sorting/sort_df.py
"""

import pandas as pd
import pyarrow as pa
from pyarrow import plasma

SAM_FIELDS = ["QNAME", "FLAG", "RNAME", "POS", "MAPQ", "CIGAR",
              "RNEXT", "PNEXT", "TLEN", "SEQ", "QUAL", "OPTIONAL"]


def put_df_to_object_store(df, client):
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

    # Generate an ID and allocate a buffer in the object store for the serialized DataFrame
    object_id = plasma.ObjectID(b"0FF1CE00C0FFEE00BEEF")

    buf = client.create(object_id, data_size)

    # Write the serialized DataFrame to the object store
    stream = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)

    return object_id


if __name__ == "__main__":
    with open("sample.sam") as f:
        lines = f.readlines()

    split_lines = []
    for line in lines:
        split_line = line.split("\t", len(SAM_FIELDS)-1)
        split_line[-1] = split_line[-1].strip("\n")
        split_lines.append(split_line)

    df = pd.DataFrame.from_records(split_lines, columns=SAM_FIELDS)

    df = df.astype({"FLAG": "int64", "POS": "int64", "MAPQ": "int64",
                    "PNEXT": "int64", "TLEN": "int64"})
    rb = pa.RecordBatch.from_pandas(df)

    client = plasma.connect("/tmp/plasma")
    object_id = put_df_to_object_store(df, client)
