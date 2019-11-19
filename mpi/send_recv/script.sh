#!/bin/bash
#SBATCH -J my_first_job
#SBATCH -n 2
#SBATCH -t 00:05:00

module load pre2019
module load mpi

srun -n 2 ./send_recv
