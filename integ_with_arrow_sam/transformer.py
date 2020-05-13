import collections
from datetime import datetime
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


def generate_object_id(chromo):
    id_base = b"LOCALGENCHROMO"
    id_suffix = b"PADD"
    chr_id = convert_chromo_name(chromo)
    encoded_chr_id = str(chr_id).zfill(2).encode("ASCII")
    object_id = plasma.ObjectID(id_base + encoded_chr_id + id_suffix)
    return object_id


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

    dispatch_plan = collections.defaultdict(dict)
    with open("chromo_destination.txt", "r") as f:
        chromo_destinations = f.readlines()
        for entry in chromo_destinations:
            chromo, dest = entry.strip("\n").split(":")
            dispatch_plan[chromo]["destination"] = dest

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

    for chromo in gb.groups:
        # Filter out invalid chromosomes
        if chromo in VALID_CHROMO_NAMES:
            # copy() is to suppress SettingWithCopyWarning
            per_chromo_sam = gb.get_group(chromo).copy()
            # For valid chromosomes, changes its RNAME (to make subsequent sorting faster)
            per_chromo_sam["RNAME"] = per_chromo_sam["RNAME"].map(convert_chromo_name)
            per_chromo_sam = per_chromo_sam.astype({"FLAG": "int64", "RNAME": "int64", "POS": "int64",
                                                    "MAPQ": "int64", "PNEXT": "int64", "TLEN": "int64"})
            obj_id = generate_object_id(chromo)
            put_df_to_plasma(client,
                             per_chromo_sam,
                             obj_id)
            dispatch_plan[chromo]["object_id"] = obj_id.binary().decode()

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Finished putting data to Plasma\n")

    with open(host_name+"_dispatch_plan.txt", "w") as f:
        for chromo in dispatch_plan:
            f.write(chromo + ":" +
                    dispatch_plan[chromo]["destination"] + "," +
                    dispatch_plan[chromo]["object_id"] + "\n")

    with open("nodes_ready_for_flight.txt", "a") as f:
        f.write(host_name + "\n")
