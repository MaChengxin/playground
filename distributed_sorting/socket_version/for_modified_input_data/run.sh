#!/bin/bash

# This is the script that every node should run by running 
# srun run.sh
# on the "master" node.

module load pre2019 Miniconda3 && source activate mcxenv && cd ~/mcx/distributed_sorting/socket_and_pkl_all_ints

# Putting the following line in this script will lead to a problem: ConnectionRefusedError: [Errno 111] Connection refused
# Instead of doing this, open a terminal for each node and run the following command manually
# python3 receiver.py -n 4 && python3 retrieve_and_sort.py &
python3 partition_and_send.py --map_file host_inputfile_map.txt --host_file nodelist.txt --port 5001
