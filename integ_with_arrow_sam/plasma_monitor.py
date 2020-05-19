import collections
from datetime import datetime
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


def is_local_objs_ready(all_obj_ids, expected_local_obj_ids):
    for obj_id in expected_local_obj_ids:
        if not obj_id in all_obj_ids:
            return False
    return True


def is_received_objs_ready(all_obj_ids, expected_num_recved_objs):
    received_obj_ids = [obj_id for obj_id in all_obj_ids if "RECEIVEDCHROMO" in obj_id]
    return len(received_obj_ids) == expected_num_recved_objs


if __name__ == "__main__":
    host_name = socket.gethostname().strip(".bullx")
    log_file = host_name + "_plasma_monitor.log"

    dispatch_plan = read_dispatch_plan_file("dispatch_plan.txt")

    expected_local_obj_ids = []
    to_remote_obj_ids = []
    for chromo in dispatch_plan:
        if dispatch_plan[chromo]["destination"] == host_name:
            expected_local_obj_ids.append(dispatch_plan[chromo]["object_id"])
        else:
            to_remote_obj_ids.append(dispatch_plan[chromo]["object_id"])

    nodes = set([dispatch_plan[chromo]["destination"] for chromo in dispatch_plan])
    expected_num_recved_objs = (len(nodes) - 1) * len(expected_local_obj_ids)

    client = plasma.connect("/tmp/plasma")
    all_obj_ids = []
    while not (is_local_objs_ready(all_obj_ids, expected_local_obj_ids) and is_received_objs_ready(all_obj_ids, expected_num_recved_objs)):
        all_objs_in_plasma = client.list()
        all_obj_ids = []
        for obj_id in all_objs_in_plasma:
            all_obj_ids.append(obj_id.binary().decode())

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Got all objects needed for the next phase. ")
        f.write("Number of objects in Plasma: " +
                str(len(all_objs_in_plasma)) + "\n")

    ids_obj_to_be_retrieved = set(all_obj_ids) - set(to_remote_obj_ids)

    with open(log_file, "a") as f:
        f.write("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]: ")
        f.write("Number of objects to be retrieved for the next phase: " +
                str(len(ids_obj_to_be_retrieved)) + "\n")

    with open(host_name+"_objs_to_be_retrieved.txt", "w") as f:
        for obj_id in ids_obj_to_be_retrieved:
            f.write(obj_id + "\n")

    subprocess.call(["python3", "sorter.py"])
