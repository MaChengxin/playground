import collections
from datetime import datetime
import socket

import pandas as pd
import pyarrow as pa
from pyarrow import plasma
from plasma_access import get_record_batch_from_plasma, put_df_to_plasma


def revert_chromo_name(chromo_idx):
    if 0 < chromo_idx < 23:
        return "chr"+str(chromo_idx)
    elif chromo_idx == 23:
        return "chrX"
    elif chromo_idx == 24:
        return "chrY"
    elif chromo_idx == 25:
        return "chrM"


def sort_chromo(object_ids, plasma_client, log_file):
    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(revert_chromo_name(int(chromo)) +
                ": started getting RB from Plasma\n")

    record_batches = [get_record_batch_from_plasma(object_id, plasma_client)
                      for object_id in object_ids]

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(revert_chromo_name(int(chromo)) +
                ": finished getting RB from Plasma, started converting RB to DF\n")

    dfs = [rb.to_pandas() for rb in record_batches]

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(revert_chromo_name(int(chromo)) +
                ": finished converting from RB to DF, started merging\n")

    merged_sam_records = pd.concat(dfs)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(revert_chromo_name(int(chromo)) +
                ": finished merging, started sorting\n")

    merged_sam_records.sort_values(by=["POS"], inplace=True)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(revert_chromo_name(int(chromo)) +
                ": finished sorting, started putting back to Plasma\n")

    put_df_to_plasma(plasma_client, merged_sam_records,
                     plasma.ObjectID.from_random())

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write(revert_chromo_name(int(chromo)) +
                ": finished putting back to Plasma\n")


if __name__ == "__main__":
    host_name = socket.gethostname().strip(".bullx")
    log_file = host_name + "_sorter.log"

    object_ids_per_chromo = collections.defaultdict(list)
    with open(host_name+"_objs_to_be_retrieved.txt", "r") as f:
        ids = set(f.readlines())
        for i in ids:
            # example i: 'RECEIVEDCHROMO03v0CP\n'
            chromo = i[14:16]
            object_ids_per_chromo[chromo].append(
                plasma.ObjectID(i.strip("\n").encode("ASCII")))

    client = plasma.connect("/tmp/plasma")

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("start serial sorting\n")

    for chromo in object_ids_per_chromo:
        sort_chromo(object_ids_per_chromo[chromo], client, log_file)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("serial sorting finished\n")
