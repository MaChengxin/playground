import argparse
import collections
from datetime import datetime
import socket
import subprocess

import pandas as pd
import pyarrow as pa
from pyarrow import plasma

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_file", help="The input SAM file.")

SAM_FIELDS = ("QNAME", "FLAG", "RNAME", "POS", "MAPQ", "CIGAR",
              "RNEXT", "PNEXT", "TLEN", "SEQ", "QUAL", "OPTIONAL")

VALID_CHROMO_NAMES = ("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
                      "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                      "chr18", "chr19", "chr20", "chr21", "chr22", "chrM", "chrX", "chrY")


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


def generate_object_id(offset):
    id_base = b"0FF1CEBEEFC0FFEE"
    encocded_offset = str(offset).zfill(4).encode("ASCII")
    object_id = plasma.ObjectID(id_base+encocded_offset)
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
    args = parser.parse_args()
    host_name = socket.gethostname().strip(".bullx")
    log_file = host_name + "_flight_sender.log"

    dispatch_plan = collections.defaultdict(dict)
    with open("chromo_destination.txt", "r") as f:
        chromo_destinations = f.readlines()
        for entry in chromo_destinations:
            chromo, dest = entry.strip("\n").split(":")
            dispatch_plan[chromo]["destination"] = dest

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Started reading SAM data from file\n")

    sam_df = read_sam_from_file(args.input_file)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Finished reading SAM data from file, started putting data to Plasma\n")

    gb = sam_df.groupby("RNAME")
    client = plasma.connect("/tmp/plasma")

    for i, chromo in enumerate(gb.groups):
        # Filter out invalid chromosomes
        if chromo in VALID_CHROMO_NAMES:
            # copy() is to suppress SettingWithCopyWarning
            per_chromo_sam = gb.get_group(chromo).copy()
            # For valid chromosomes, changes its RNAME (to make subsequent sorting faster)
            per_chromo_sam["RNAME"] = per_chromo_sam["RNAME"].map(convert_chromo_name)
            per_chromo_sam = per_chromo_sam.astype({"FLAG": "int64", "RNAME": "int64", "POS": "int64",
                                                    "MAPQ": "int64", "PNEXT": "int64", "TLEN": "int64"})
            obj_id = generate_object_id(i)
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

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Start the cpp app send-to-dest\n")

    subprocess.call("./send-to-dest")
