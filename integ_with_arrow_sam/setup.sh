#!/bin/bash

# This is the script that only needs to be run on the coordination node.

# Allocate 2 nodes
salloc -N 2 -p normal -t 60
nodeset -S  "," -e $SLURM_NODELIST > nodelist.txt

# Issue the following commands in the Singularity container
python3 distribute_workload.py
make
