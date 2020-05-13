#!/bin/bash

# This is the script that only needs to be run on the coordination node.

# Allocate 4 nodes, change the number if needed
salloc -N 4 -p normal -t 60
nodeset -S  "," -e $SLURM_NODELIST > nodelist.txt
touch nodes_ready_for_flight.txt

# Issue the following commands in the Singularity container
# Practically it can be done by ssh to a compute node and start Singularity there
python3 assign_input_file.py -p "/scratch-shared/tahmad/bio_data/SAM/header_removed/U0a_CGATGT_split/4_nodes" && python3 distribute_workload.py && make
