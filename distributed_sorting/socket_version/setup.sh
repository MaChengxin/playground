#!/bin/bash

# This is the script that only need to be run on the "master" node.

salloc -N 4 -p normal -t 60
nodeset -S  "," -e $SLURM_NODELIST > nodelist.txt
module load pre2019 Miniconda3 && source activate mcxenv
python3 gen_map.py
