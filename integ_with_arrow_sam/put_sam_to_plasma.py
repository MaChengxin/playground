""" Script to simulate BWA in ArrowSAM.
Useful links:
    https://github.com/apache/arrow/blob/master/python/examples/plasma/sorting/sort_df.py
"""

import collections
import socket

import pandas as pd
import pyarrow as pa
from pyarrow import plasma

from shared_info import SAM_FIELDS, VALID_CHROMO_NAMES, WORKLOAD_DISTRIBUTION

def put_df_to_object_store(client, df, object_id):
    """
    Precondition: the Plasma Object Store has been opened.
    e.g. by: plasma_store -m 1000000000 -s /tmp/plasma
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


def generate_object_id(offset):
    id_base = b"0FF1CEBEEFC0FFEE"
    encocded_offset = str(offset).zfill(4).encode("ASCII")
    object_id = plasma.ObjectID(id_base+encocded_offset)
    return object_id


if __name__ == "__main__":
    with open("nodeslist.txt", "r") as f:
        nodes = f.readline().split(",")
        num_of_nodes = len(nodes)

    dispatch_plan = collections.defaultdict(dict)

    # The logic of assigning destinations for the chromosomes goes here.
    chromo_groups = WORKLOAD_DISTRIBUTION[num_of_nodes]
    for i, group in enumerate(chromo_groups):
        for chromo in group:
            dispatch_plan[chromo]["destination"] = nodes[i]

    with open("2k_reads.sam", "r") as f:
        lines = f.readlines()

        split_lines = []
        for line in lines:
            split_line = line.split("\t", len(SAM_FIELDS)-1)
            split_line[-1] = split_line[-1].strip("\n")
            split_lines.append(split_line)

        df = pd.DataFrame.from_records(split_lines, columns=SAM_FIELDS)

        df = df.astype({"FLAG": "int64", "POS": "int64", "MAPQ": "int64",
                        "PNEXT": "int64", "TLEN": "int64"})

    gb = df.groupby("RNAME")

    client = plasma.connect("/tmp/plasma")

    for i, chromo in enumerate(gb.groups):
        if chromo in VALID_CHROMO_NAMES:
            obj_id = generate_object_id(i)
            put_df_to_object_store(client,
                                   gb.get_group(chromo),
                                   obj_id)
            dispatch_plan[chromo]["object_id"] = obj_id.binary().decode()

    with open(socket.gethostname()+"_dispatch_plan.txt", "w") as f:
        for chromo in dispatch_plan:
            f.write(chromo + ":" +
                    dispatch_plan[chromo]["destination"] + "," +
                    dispatch_plan[chromo]["object_id"] + "\n")
