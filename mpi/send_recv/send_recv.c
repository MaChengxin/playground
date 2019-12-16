/*
This is based on one of the tutorials on mpitutorial.com.
Author: Wes Kendall
Source code link: https://github.com/wesleykendall/mpitutorial/blob/gh-pages/tutorials/mpi-send-and-receive/code/send_recv.c
*/

#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define DATA_SOURCE_RANK 0
#define DATA_DESTINATION_RANK 1

typedef struct
{
  int64_t a;
  int64_t b;
  int64_t c;
  int64_t d;
} Record;

const int32_t MB = 1 << 20;

int main(int argc, char **argv)
{
  // Initialize the MPI environment
  MPI_Init(NULL, NULL);

  // Find out rank, size
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  int world_size;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);

  // We are assuming at least 2 processes for this task
  if (world_size < 2)
  {
    fprintf(stderr, "World size must be greater than 1 for %s\n", argv[0]);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  // Get the name of the processor, to be printed later
  char processor_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(processor_name, &name_len);

  // Construct MPI's version of type Record
  int count = 4;
  int block_lengths[4] = {1, 1, 1, 1};
  MPI_Aint offsets[4];
  offsets[0] = offsetof(Record, a);
  offsets[1] = offsetof(Record, b);
  offsets[2] = offsetof(Record, c);
  offsets[3] = offsetof(Record, d);
  MPI_Datatype types[4] = {MPI_INT64_T, MPI_INT64_T, MPI_INT64_T, MPI_INT64_T};
  MPI_Datatype MPI_Record;

  MPI_Type_create_struct(count,
                         block_lengths,
                         offsets,
                         types,
                         &MPI_Record);
  MPI_Type_commit(&MPI_Record);

  int MPI_Record_size;
  MPI_Type_size(MPI_Record, &MPI_Record_size);

  // Will be used for timing
  double t1, t2;

  char *temp = argv[1];
  int num_of_records = atoi(temp);

  if (rank == DATA_SOURCE_RANK)
  {
    Record *src_arr = malloc(num_of_records * sizeof(Record));

    int i;
    for (i = 0; i < num_of_records; i++)
    {
      src_arr[i] = (Record){42, 42, 42, 42};
    }

    t1 = MPI_Wtime();

    MPI_Send(
        /* data         = */ src_arr,
        /* count        = */ num_of_records,
        /* datatype     = */ MPI_Record,
        /* destination  = */ DATA_DESTINATION_RANK,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD);

    t2 = MPI_Wtime();

    printf("Sending took %f on %s \n", t2 - t1, processor_name);
    printf("Sent data in bytes %d \n", num_of_records * MPI_Record_size);
    printf("Speed of data sending %f (MB/s)\n", num_of_records * MPI_Record_size / MB / (t2 - t1));

    FILE *fp;
    fp = fopen("MPI_send_recv_records_SEND.log", "a");
    fprintf(fp, "%d \t %f \n", num_of_records, num_of_records * MPI_Record_size / MB / (t2 - t1));
    fclose(fp);
  }

  else if (rank == DATA_DESTINATION_RANK)
  {
    Record *recv_arr = malloc(num_of_records * sizeof(Record));

    t1 = MPI_Wtime();

    MPI_Recv(
        /* data         = */ recv_arr,
        /* count        = */ num_of_records,
        /* datatype     = */ MPI_Record,
        /* source       = */ DATA_SOURCE_RANK,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD,
        /* status       = */ MPI_STATUS_IGNORE);

    t2 = MPI_Wtime();

    printf("Receiving took %f on %s \n", t2 - t1, processor_name);

    if (recv_arr[num_of_records - 1].c == 42)
    {
      printf("Verification of sample data: pass \n");
      printf("Received data in bytes %d \n", num_of_records * MPI_Record_size);
      printf("Speed of data receiving %f (MB/s)\n", num_of_records * MPI_Record_size / MB / (t2 - t1));

      FILE *fp;
      fp = fopen("MPI_send_recv_records_RECV.log", "a");
      fprintf(fp, "%d \t %f \n", num_of_records, num_of_records * MPI_Record_size / MB / (t2 - t1));
      fclose(fp);
    }
    else
    {
      printf("Verification of sample data: failed \n");
    }
  }

  MPI_Finalize();
}
