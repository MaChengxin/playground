import pyarrow as pa

def put_df_to_plasma(client, df, object_id):
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


def get_record_batch_from_plasma(object_id, client):
    """ https://arrow.apache.org/docs/python/plasma.html#getting-pandas-dataframes-from-plasma
    """
    [buffer] = client.get_buffers([object_id])
    buffer_reader = pa.BufferReader(buffer)
    reader = pa.RecordBatchStreamReader(buffer_reader)
    record_batch = reader.read_next_batch()

    return record_batch
