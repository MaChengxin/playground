import collections
import re
import socket
import subprocess
import time
from pyarrow import plasma


def read_dispatch_plan_file(file_name):
    dispatch_plan = collections.defaultdict(dict)
    with open(file_name, "r") as f:
        dispatch_plan_entries = f.readlines()
        for entry in dispatch_plan_entries:
            chromo, dest, object_id = re.split(":|,", entry.strip("\n"))
            dispatch_plan[chromo]["destination"] = dest
            dispatch_plan[chromo]["object_id"] = object_id
    return dispatch_plan


def read_chromo_dest_file(file_name):
    dest_chromo = collections.defaultdict(list)
    with open(file_name, "r") as f:
        chromo_dests = f.readlines()
        for entry in chromo_dests:
            chromo, dest = entry.strip("\n").split(":")
            dest_chromo[dest].append(chromo)
    return dest_chromo


if __name__ == "__main__":
    host_name = socket.gethostname().strip(".bullx")

    dest_chromo = read_chromo_dest_file("chromo_destination.txt")
    num_of_nodes = len(dest_chromo)
    num_plasma_obj_to_receive = (
        num_of_nodes - 1) * len(dest_chromo[host_name])

    client = plasma.connect("/tmp/plasma")
    all_objects_in_plasma = client.list()
    # There are 25 objects (1-22, X,Y,M) put by BWA to Plasma.
    while len(all_objects_in_plasma) < 25 + num_plasma_obj_to_receive:
        all_objects_in_plasma = client.list()
        time.sleep(1)

    print("Got all objects needed for the next phase.")
    print("Number of objects in Plasma: ", len(all_objects_in_plasma))

    ids_all_objects = []
    for obj_id in all_objects_in_plasma:
        ids_all_objects.append(obj_id.binary().decode())

    ids_sent_away_objects = []
    dispatch_plan = read_dispatch_plan_file(host_name+"_dispatch_plan.txt")
    for chromo in dispatch_plan:
        if dispatch_plan[chromo]["destination"] != host_name:
            ids_sent_away_objects.append(dispatch_plan[chromo]["object_id"])

    ids_obj_to_be_retrieved = set(ids_all_objects) - set(ids_sent_away_objects)
    print("Number of objects to be retrieved for the next phase: ",
          len(ids_obj_to_be_retrieved))

    with open(host_name+"_objs_to_be_retrieved.txt", "w") as f:
        for obj_id in ids_obj_to_be_retrieved:
            f.write(obj_id + "\n")

    subprocess.call(["python3", "retrieve_and_sort.py"])
