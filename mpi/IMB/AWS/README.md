# Source
Code: [intel/mpi-benchmarks](https://github.com/intel/mpi-benchmarks)

User guide: https://software.intel.com/en-us/imb-user-guide

# Platform
4 t3a.xlarge instances on AWS.
Setup is based on [this reference](https://help.ubuntu.com/community/MpichCluster).

# Note
On November 28, 2019, the MPI implementation installed on these nodes are changed from MPICH to OpenMPI
(to use [`Rankfiles`](https://www.open-mpi.org/doc/v4.0/man1/mpirun.1.php#sect10)).
