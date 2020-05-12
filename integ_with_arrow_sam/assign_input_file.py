""" This script generates a mapping between hosts and input files.
"""

import argparse

from os import listdir
from os.path import isfile, join

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--path", help="Path to input files.")

if __name__ == "__main__":
    args = parser.parse_args()

    with open("nodelist.txt") as f:
        nodes = f.readline()
    
    path = args.path
    # Assume there is no extra files in this directory
    inputfiles = [join(path, f) for f in listdir(path) if isfile(join(path, f))]
    assigned_input = {n: inputfiles[i] for i, n in enumerate(nodes.strip("\n").split(","))}

    with open("assigned_inputfiles.txt", "w") as f:
        for host, inputfile in assigned_input.items():
            f.write("{0}:{1}\n".format(host, inputfile))
