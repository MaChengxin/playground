""" Script for processing data in SAM format.
Useful links:
    https://github.com/apache/arrow/blob/master/python/examples/plasma/sorting/sort_df.py
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma

SAM_FIELDS = ['QNAME', 'FLAG', 'RNAME', 'POS', 'MAPQ',
              'CIGAR', 'RNEXT', 'PNEXT', 'TLEN', 'SEQ']


def crop_input_sam(sam_file):
    with open(sam_file) as input_file:
        lines = input_file.readlines()
        cropped_lines = ['\t'.join(l.split('\t')[:10])+'\n' for l in lines]
        with open('cropped_'+sam_file, 'w') as output_file:
            output_file.writelines(cropped_lines)


def put_df_to_object_store(df, client):
    """ Precondition: the Plasma Object Store has been opened.
    Returns the object ID.
    """
    record_batch = pa.RecordBatch.from_pandas(df)

    # the primary intent of using a Arrow stream is to pass it to other Arrow facilities that will make use of it, such as Arrow IPC routines
    # https://arrow.apache.org/docs/python/generated/pyarrow.NativeFile.html#pyarrow.NativeFile

    # Get size of record batch and schema
    mock_output_stream = pa.MockOutputStream()
    # MockOutputStream is a helper class to tracks the size of allocations.
    # Writes to this stream do not copy or retain any data,
    # they just bump a size counter that can be later used to know exactly which data size needs to be allocated for actual writing.
    stream_writer = pa.RecordBatchStreamWriter(
        mock_output_stream, record_batch.schema)
    stream_writer.write_batch(record_batch)
    data_size = mock_output_stream.size()

    # Generate an ID and allocate a buffer in the object store for the
    # serialized DataFrame
    object_id = plasma.ObjectID(b"0FF1CE00C0FFEE00BEEF")

    buf = client.create(object_id, data_size)

    # Write the serialized DataFrame to the object store
    # A FixedSizeBufferWriter is a stream writing to a Arrow buffer.
    # https://arrow.apache.org/docs/python/generated/pyarrow.FixedSizeBufferWriter.html
    stream = pa.FixedSizeBufferWriter(buf)
    stream_writer = pa.RecordBatchStreamWriter(stream, record_batch.schema)
    stream_writer.write_batch(record_batch)

    # Seal the object
    client.seal(object_id)

    return object_id


if __name__ == "__main__":
    crop_input_sam('xin')
    df = pd.read_csv('cropped_xin', delimiter='\t', names=SAM_FIELDS)

    client = plasma.connect('/tmp/store')
    object_id = put_df_to_object_store(df, client)
