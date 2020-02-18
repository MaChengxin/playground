""" This script generates a mapping between hostnames and input files.
"""


if __name__ == "__main__":
    with open("nodelist.txt") as f:
        nodes = f.readline()
        host_inputfile_map = {n: "~/mcx/distributed_sorting_data/new/4_nodes/records_on_node_"+str(i)+".txt"
                              for i, n in enumerate(nodes.strip("\n").split(","))}

    with open("host_inputfile_map.txt", "w") as f:
        for host, inputfile in host_inputfile_map.items():
            f.write("{0}:{1}\n".format(host, inputfile))
