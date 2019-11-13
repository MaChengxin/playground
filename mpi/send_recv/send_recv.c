/*
This is based on one of the tutorials on mpitutorial.com.
Author: Wes Kendall
Source code link: https://github.com/wesleykendall/mpitutorial/blob/gh-pages/tutorials/mpi-send-and-receive/code/send_recv.c
*/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);

  // Find out rank, size
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // We are assuming at least 2 processes for this task
  if (world_size < 2)
  {
    fprintf(stderr, "World size must be greater than 1 for %s\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int num_of_ints = 40000;
  double t1, t2;

  if (world_rank == 0)
  {
    int *int_arr = (int *)malloc(num_of_ints * sizeof(int));

    int i;
    for (i = 0; i < num_of_ints; i++)
    {
      int_arr[i] = i;
    }

    t1 = MPI_Wtime();

    MPI_Send(
        /* data         = */ int_arr,
        /* count        = */ num_of_ints,
        /* datatype     = */ MPI_INT,
        /* destination  = */ 1,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD);

    t2 = MPI_Wtime();

    printf("Sending took %f \n", t2 - t1);
  }

  else if (world_rank == 1)
  {
    int *int_arr = (int *)malloc(num_of_ints * sizeof(int));

    t1 = MPI_Wtime();

    MPI_Recv(
        /* data         = */ int_arr,
        /* count        = */ num_of_ints,
        /* datatype     = */ MPI_INT,
        /* source       = */ 0,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD,
        /* status       = */ MPI_STATUS_IGNORE);

    t2 = MPI_Wtime();

    printf("Receiving took %f \n", t2 - t1);
  }

  MPI_Finalize();
}
