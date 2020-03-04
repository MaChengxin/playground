#!/bin/bash

# Open a terminal for each node and run the following commands manually
# module load pre2019 Miniconda3 && source activate mcxenv && cd ~/mcx/arrow/cpp/my_flight/debug/
# ./plasma-store-server -m 8000000000 -s /tmp/plasma &
# ./receive-and-store -num_nodes 4 &

# This is the command that every node should run by running
# srun run.sh
# on the "master" node.
python3 partition_and_send.py --map_file host_inputfile_map.txt -p 0,16,32,48,64 --host_file nodelist.txt
