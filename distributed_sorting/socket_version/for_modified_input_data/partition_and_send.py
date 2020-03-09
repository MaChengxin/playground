import argparse
from datetime import datetime
from multiprocessing import Process
import os
import pandas as pd
from sender import send_file
import socket

parser = argparse.ArgumentParser()
parser.add_argument("--map_file",
                    help="The file containing mapping between hostnames and input files.")
parser.add_argument(
    "--host_file", help="The file containing the hostnames of the receivers")
parser.add_argument(
    "--port", help="Port to use, default is 5001", default=5001)


def pickle_records_to_file(records, file_name):
    records.to_pickle(file_name)


if __name__ == "__main__":
    args = parser.parse_args()

    with open(args.host_file) as host_file:
        hosts = host_file.readline()
    hosts = hosts.strip("\n").split(",")
    port = int(args.port)

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")

    with open(args.map_file) as map_file:
        mapping = map_file.readlines()
        mapping = dict(m.strip("\n").split(":") for m in mapping)

    records = pd.read_csv(mapping[socket.gethostname().split(".")[0]],
                          sep="\t",
                          names=["key", "data"])

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished reading input file, started partitioning the records\n")

    # Reference: https://stackoverflow.com/questions/21441259/pandas-groupby-range-of-values
    gb = records.groupby(pd.cut(records["key"], len(hosts)))
    to_be_pickled = [gb.get_group(g) for g in gb.groups]

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished partitioning the records, started serializing the data to pkl files\n")

    if not os.path.exists(socket.gethostname()+"/to_be_sent"):
        os.makedirs(socket.gethostname()+"/to_be_sent")
    pkl_files = []

    # Parallel serialization
    procs = []
    for index, records in enumerate(to_be_pickled):
        pkl_file = socket.gethostname()+"/to_be_sent/" + \
            socket.gethostname()+"_"+str(index)+".pkl"
        pkl_files.append(pkl_file)
        proc = Process(target=pickle_records_to_file, args=(records, pkl_file))
        procs.append(proc)
        proc.daemon = True
        proc.start()

    for proc in procs:
        proc.join()

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished serializing the data to pkl files, started sending to destination nodes\n")

    # Send the files to receivers in parallel
    procs = []
    for pkl_file, host in zip(pkl_files, hosts):
        proc = Process(target=send_file, args=(pkl_file, host, port))
        procs.append(proc)
        proc.daemon = True
        proc.start()

    for proc in procs:
        proc.join()

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sending to destination nodes\n")
