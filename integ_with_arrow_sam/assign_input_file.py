""" This script generates a mapping between hosts and input files.
"""

from os import listdir
from os.path import isfile, join


if __name__ == "__main__":
    with open("nodelist.txt") as f:
        nodes = f.readline()
    
    path = "/home/tahmad/mcx/bio_data/ERR001268/4_nodes"
    # Assume there is no extra files in this directory
    inputfiles = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
    assigned_input = {n: inputfiles[i] for i, n in enumerate(nodes.strip("\n").split(","))}

    with open("assigned_inputfiles.txt", "w") as f:
        for host, inputfile in assigned_input.items():
            f.write("{0}:{1}\n".format(host, inputfile))
