import argparse
from datetime import datetime
import gc
import psutil
import time

import pandas as pd
from pyarrow import plasma

from transformer import VALID_CHROMO_NAMES, read_sam_from_file, read_dispatch_plan_file, convert_chromo_name
from plasma_access import put_df_to_plasma, get_record_batch_from_plasma


parser = argparse.ArgumentParser()
parser.add_argument("--input_file", help="The input SAM file.")

def print_mem_usage(event):
    mem_usage = "{:.2f}".format(psutil.virtual_memory().available / (1024 ** 3))
    log_msg = "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: " + event + ", available memory: " + mem_usage + " GB"
    print(log_msg)
    with open("mem_profiler.log", "a") as f:
        f.write(log_msg + "\n")


def print_df_size(df, df_name):
    """ This function might take up two much time. """
    size = "{:.2f}".format(df.memory_usage(deep=True).sum() / (1024 ** 3))
    print("Size of " + df_name + ": " + size + " GB")

if __name__ == "__main__":
    args = parser.parse_args()
    print_mem_usage("Start of the profiler")
    with open("mem_profiler.log", "a") as f:
        f.write("Input file: " + args.input_file + "\n")

    input_sam_df = read_sam_from_file(args.input_file)
    print_mem_usage("After reading SAM from file")
    # print_df_size(input_sam_df, "input_sam_df")

    client = plasma.connect("/tmp/plasma")

    gb = input_sam_df.groupby("RNAME")
    dispatch_plan = read_dispatch_plan_file("dummy_dispatch_plan.txt")

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

    print_mem_usage("After filtering and putting DF into Plasma")

    input_sam_df = []
    gc.collect()
    time.sleep(5) # Give the GC some time
    print_mem_usage("After trying to free memory occupied by input_sam_df")
    
    rbs = []
    for chromo in VALID_CHROMO_NAMES:
        obj_id = plasma.ObjectID(dispatch_plan[chromo]["object_id"].encode("ASCII"))
        rbs.append(get_record_batch_from_plasma(obj_id, client))

    print_mem_usage("After getting RBs from Plasma")

    client.disconnect()

    dfs = [rb.to_pandas() for rb in rbs]
    print_mem_usage("After converting RBs to DFs")

    rbs = []
    gc.collect()
    time.sleep(5) # Give the GC some time
    print_mem_usage("After trying to free memory occupied by rbs")

    for proc in psutil.process_iter():
        if "plasma-store" in proc.name():
            proc.kill()
            print("Plasma Store process killed.")
    time.sleep(60)      
    print_mem_usage("After killing the Plasma Store process")

    merged_sam_df = pd.concat(dfs)
    print_mem_usage("After merging DFs")
    # print_df_size(merged_sam_df, "merged_sam_df")

    dfs = []
    gc.collect()
    time.sleep(5) # Give the GC some time
    print_mem_usage("After trying to free memory occupied by dfs")

    merged_sam_df.sort_values(by=["RNAME"], inplace=True)
    print_mem_usage("After sorting merged DF")
    # print_df_size(merged_sam_df, "merged_sam_df")

    merged_sam_df = []
    gc.collect()
    print_mem_usage("After trying to free memory occupied by merged DF")

    print_mem_usage("End of the profiler")
