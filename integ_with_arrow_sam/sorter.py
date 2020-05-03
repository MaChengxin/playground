from datetime import datetime
import socket

import pandas as pd
import pyarrow as pa
from pyarrow import plasma


def get_record_batch_from_plasma(object_id, client):
    """ https://arrow.apache.org/docs/python/plasma.html#getting-pandas-dataframes-from-plasma
    """
    [buffer] = client.get_buffers([object_id])
    buffer_reader = pa.BufferReader(buffer)
    reader = pa.RecordBatchStreamReader(buffer_reader)
    record_batch = reader.read_next_batch()

    return record_batch


if __name__ == "__main__":
    host_name = socket.gethostname().strip(".bullx")
    log_file = host_name + "_sorter.log"

    with open(host_name+"_objs_to_be_retrieved.txt", "r") as f:
        ids = set(f.readlines())
        # Why encode? See: https://stackoverflow.com/a/51890365/5723556
        object_ids = [plasma.ObjectID(id.strip("\n").encode()) for id in ids]

    client = plasma.connect("/tmp/plasma")

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("started retrieving received records from Plasma\n")

    record_batches = [get_record_batch_from_plasma(object_id, client)
                      for object_id in object_ids]

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(
            "finished retrieving received records from Plasma, started converting to DataFrame\n")

    dfs = [rb.to_pandas() for rb in record_batches]

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("finished converting to DataFrame, started concatenating\n")

    all_sam_records = pd.concat(dfs)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("finished concatenating, started sorting the records\n")

    all_sam_records.sort_values(by=["RNAME", "POS"], inplace=True)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("finished sorting the records, started writing to csv\n")

    all_sam_records.reset_index(drop=True, inplace=True)
    all_sam_records.to_csv(host_name+"_sorted.sam",
                           sep="\t", header=False, index=False)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("finished writing to csv \n")
