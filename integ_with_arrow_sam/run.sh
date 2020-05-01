#!/bin/bash

# Open a terminal for each node and run the following commands manually:
# plasma_store -m 8000000000 -s /tmp/plasma &
# python3 plasma_monitor.py &
# ./receive-and-store &

# This is the command that every node should run by issuing
# `srun run.sh`
# on the coordination node.
# Note that this to simulate the output of modified BWA in ArrowSAM.
# We assume that SAM data is already prepared by the original BWA in our simulator.
python3 put_sam_to_plasma_and_send.py
