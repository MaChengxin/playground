#!/bin/bash

# Open a terminal for each node, shell into the Singularity container, and run the following commands manually:
# cd ~/mcx/playground/integ_with_arrow_sam && singularity shell ../../integ_arrowsam_simulator_202005201656.simg
# plasma_store -m 30000000000 -s /tmp/plasma &
# python3 plasma_monitor.py &
# python3 synchronizer.py &
# ./flight-receiver &

# This is the command that every node should run by issuing
# `srun run.sh`
# on the coordination node.
# Note that this to simulate the output of modified BWA in ArrowSAM.
# We assume that SAM data is already prepared by the original BWA in our simulator.
cd ~/mcx/playground/integ_with_arrow_sam
singularity exec ../../integ_arrowsam_simulator_202005201656.simg python3 transformer.py
