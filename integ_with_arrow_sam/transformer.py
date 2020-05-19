import collections
from datetime import datetime
import re
import socket

import pandas as pd
from pyarrow import plasma
from plasma_access import put_df_to_plasma

SAM_FIELDS = ("QNAME", "FLAG", "RNAME", "POS", "MAPQ", "CIGAR",
              "RNEXT", "PNEXT", "TLEN", "SEQ", "QUAL", "OPTIONAL")

VALID_CHROMO_NAMES = ("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
                      "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                      "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM")


def read_sam_from_file(filename):
    with open(filename, "r") as f:
        lines = f.readlines()

        split_lines = []
        for line in lines:
            split_line = line.split("\t", len(SAM_FIELDS)-1)
            split_line[-1] = split_line[-1].strip("\n")
            split_lines.append(split_line)

        df = pd.DataFrame.from_records(split_lines, columns=SAM_FIELDS)

    return df


def read_dispatch_plan_file(file_name):
    dispatch_plan = collections.defaultdict(dict)
    with open(file_name, "r") as f:
        dispatch_plan_entries = f.readlines()
        for entry in dispatch_plan_entries:
            chromo, dest, object_id = re.split(":|,", entry.strip("\n"))
            dispatch_plan[chromo]["destination"] = dest
            dispatch_plan[chromo]["object_id"] = object_id
    return dispatch_plan


def convert_chromo_name(chromo):
    chromo = chromo.strip("chr")
    if chromo == "X":
        return 23
    elif chromo == "Y":
        return 24
    elif chromo == "M":
        return 25
    else:
        return int(chromo)


if __name__ == "__main__":
    host_name = socket.gethostname().strip(".bullx")
    log_file = host_name + "_transformer.log"

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Started reading SAM data from file\n")

    with open("assigned_inputfiles.txt", "r") as f:
        host_inputfiles = f.readlines()
        for entry in host_inputfiles:
            host, inputfile = entry.strip("\n").split(":")
            if host == host_name:
                sam_df = read_sam_from_file(inputfile)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Finished reading SAM data from file, started putting data to Plasma\n")

    gb = sam_df.groupby("RNAME")
    client = plasma.connect("/tmp/plasma")

    dispatch_plan = read_dispatch_plan_file("dispatch_plan.txt")

    for chromo in gb.groups:
        # Filter out invalid chromosomes
        if chromo in VALID_CHROMO_NAMES:
            # copy() is to suppress SettingWithCopyWarning
            per_chromo_sam = gb.get_group(chromo).copy()
            # For valid chromosomes, changes its RNAME (to make subsequent sorting faster)
            per_chromo_sam["RNAME"] = per_chromo_sam["RNAME"].map(convert_chromo_name)
            per_chromo_sam = per_chromo_sam.astype({"FLAG": "int64", "RNAME": "int64", "POS": "int64",
                                                    "MAPQ": "int64", "PNEXT": "int64", "TLEN": "int64"})
            obj_id = plasma.ObjectID(dispatch_plan[chromo]["object_id"].encode("ASCII"))
            put_df_to_plasma(client,
                             per_chromo_sam,
                             obj_id)

    client.disconnect()

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Finished putting data to Plasma\n")

    with open("nodes_ready_for_flight.txt", "a") as f:
        f.write(host_name + "\n")
