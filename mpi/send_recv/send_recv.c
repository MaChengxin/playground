/*
This is based on one of the tutorials on mpitutorial.com.
Author: Wes Kendall
Source code link: https://github.com/wesleykendall/mpitutorial/blob/gh-pages/tutorials/mpi-send-and-receive/code/send_recv.c
*/

#include <mpi.h>
#include <stdbool.h>
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

bool verify_received_data(Record *recv_data, int num_of_records)
{
  int i;
  for (i = 0; i < num_of_records; i++)
  {
    if (recv_data[i].a + recv_data[i].b != 0 || recv_data[i].c != INT64_MIN || recv_data[i].d != INT64_MAX)
    {
      return false;
    }
  }
  return true;
}

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

  /*
  Construct MPI's version of type Record
  https://www.mpich.org/static/docs/latest/www3/MPI_Type_create_struct.html
  Visualization: https://www.semanticscholar.org/paper/Sur-la-validation-num%C3%A9rique-des-codes-de-calcul-(On-Montan/a008511e0c2be66735afc0f312a7b1df9d6122af/figure/4
  */

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

  double time = 0.0;

  char *num_of_records_str = argv[1];
  int num_of_records = atoi(num_of_records_str);

  if (rank == DATA_SOURCE_RANK)
  {
    Record *src_arr = malloc(num_of_records * sizeof(Record));

    int i;
    for (i = 0; i < num_of_records; i++)
    {
      src_arr[i] = (Record){i, -i, INT64_MIN, INT64_MAX};
    }

    time -= MPI_Wtime();

    /*
    Current understanding of third argument datatype (might be wrong): this is used to INTERPRET the data instead of MANIPULATING the data
    There is MPI_PACK in MPI for serialization:
    https://www.mpich.org/static/docs/latest/www3/MPI_Pack.html
    */

    MPI_Send(
        /* data         = */ src_arr,
        /* count        = */ num_of_records,
        /* datatype     = */ MPI_Record,
        /* destination  = */ DATA_DESTINATION_RANK,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD);

    time += MPI_Wtime();

    printf("Sending took %f second(s) on %s \n", time, processor_name);
    printf("Sent data in bytes %d \n", num_of_records * MPI_Record_size);
    printf("Speed of data sending %f (MB/s)\n", num_of_records * MPI_Record_size / MB / time);

    FILE *fp;
    fp = fopen("MPI_send_recv_records_SEND.log", "a");
    fprintf(fp, "%d \t %f \n", num_of_records, num_of_records * MPI_Record_size / MB / time);
    fclose(fp);
  }

  else if (rank == DATA_DESTINATION_RANK)
  {
    Record *recv_arr = malloc(num_of_records * sizeof(Record));

    time -= MPI_Wtime();

    MPI_Recv(
        /* data         = */ recv_arr,
        /* count        = */ num_of_records,
        /* datatype     = */ MPI_Record,
        /* source       = */ DATA_SOURCE_RANK,
        /* tag          = */ 0,
        /* communicator = */ MPI_COMM_WORLD,
        /* status       = */ MPI_STATUS_IGNORE);

    time += MPI_Wtime();

    printf("Receiving took %f second(s) on %s \n", time, processor_name);

    if (verify_received_data(recv_arr, num_of_records) == true)
    {
      printf("Verification of data: pass \n");
      printf("Received data in bytes %d \n", num_of_records * MPI_Record_size);
      printf("Speed of data receiving %f (MB/s)\n", num_of_records * MPI_Record_size / MB / time);

      FILE *fp;
      fp = fopen("MPI_send_recv_records_RECV.log", "a");
      fprintf(fp, "%d \t %f \n", num_of_records, num_of_records * MPI_Record_size / MB / time);
      fclose(fp);
    }
    else
    {
      printf("Verification of data: failed \n");
    }
  }

  MPI_Finalize();
}
