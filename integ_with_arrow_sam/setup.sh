#!/bin/bash

# This is the script that only needs to be run on the coordination node.

# Allocate 2 nodes
salloc -N 2 -p normal -t 60
nodeset -S  "," -e $SLURM_NODELIST > nodelist.txt

python3 distribute_workload.py

# To generate the executables
make
