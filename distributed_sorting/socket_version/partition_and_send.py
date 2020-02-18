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
parser.add_argument("-p", "--partition_boundaries",
                    help="The boundaries to partition the records.")
parser.add_argument(
    "--host_file", help="The file containing the hostnames of the receivers")
parser.add_argument(
    "--port", help="Port to use, default is 5001", default=5001)


if __name__ == "__main__":
    # Parse the arguments
    args = parser.parse_args()
    pb = args.partition_boundaries.split(',')
    pb = list(map(int, pb))
    partitioned_groups = []

    def gen_groups(low, high):
        return ['GROUP'+str(i).zfill(2) for i in range(low, high)]
    for i in range(len(pb)-1):
        partitioned_groups.append(gen_groups(pb[i], pb[i+1]))

    """ partitioned_groups will look something like this given -p 0,3,6,10:
    [['GROUP0', 'GROUP1', 'GROUP2'], ['GROUP3', 'GROUP4', 'GROUP5'], ['GROUP6', 'GROUP7', 'GROUP8', 'GROUP9']]
    (Note: this is the result before adding zfill(2))
    """

    with open(args.host_file) as host_file:
        hosts = host_file.readline()
    hosts = hosts.strip("\n").split(",")
    port = int(args.port)

    # Read the input file, construct the data to be partitioned: csv/txt -> DataFrame
    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started reading input file\n")

    with open(args.map_file) as map_file:
        mapping = map_file.readlines()
        mapping = dict(m.strip("\n").split(":") for m in mapping)

    records = pd.read_csv(mapping[socket.gethostname().split(".")[0]],
                          sep="\t",
                          names=["group_name", "seq", "data"])

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished reading input file, started partitioning the records, started groupby()\n")

    # Partition the records: DataFrame -> DataFrame
    # https://stackoverflow.com/questions/47769453/pandas-split-dataframe-to-multiple-by-unique-values-rows

    dfs = dict(tuple(records.groupby('group_name')))

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            ">> partitioning the records, groupby() finished, pd.concat() started \n")
    sub_records = []
    for i in range(len(partitioned_groups)):
        sub_records.append(pd.concat([dfs[group]
                                      for group in partitioned_groups[i]]))

    with open(socket.gethostname()+'_s.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("pd.concat() finished, finished partitioning the records, started serializing the data to pkl files\n")

    # Serialize the data to pkl files: DataFrame -> pkl
    if not os.path.exists(socket.gethostname()+"/to_be_sent"):
        os.makedirs(socket.gethostname()+"/to_be_sent")
    pkl_files = []
    for index, records in enumerate(sub_records):
        pkl_file = socket.gethostname()+"/to_be_sent/" + \
            socket.gethostname()+"_"+str(index)+".pkl"
        pkl_files.append(pkl_file)
        records.to_pickle(pkl_file)

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
