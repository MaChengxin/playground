/*
Starter code: 
    https://www.msi.umn.edu/workshops/mpi/hands-on/data-movement/gather-scatter/solution-c
Useful links: 
    https://stackoverflow.com/questions/20776797/mpi-bcast-with-mpi-type-create-struct-segmentation-fault
    https://stackoverflow.com/questions/6127503/shuffle-array-in-c
    https://stackoverflow.com/questions/8721189/how-to-sort-an-array-of-structs-in-c

*/

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"

#define NUM_OF_RECORDS 42
#define ROOT_RANK 0

typedef struct
{
    int id;
    int value;
} Record;

/* Arrange the N elements of ARRAY in random order.
   Only effective if N is much smaller than RAND_MAX;
   if this may not be the case, use a better random
   number generator. */
void shuffle(Record *array, size_t n)
{
    if (n > 1)
    {
        size_t i;
        for (i = 0; i < n - 1; i++)
        {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            int t = array[j].id;
            array[j].id = array[i].id;
            array[i].id = t;

            t = array[j].value;
            array[j].value = array[i].value;
            array[i].value = t;
        }
    }
}

// Comparator function for sorting the records
int rec_cmp(const void *v1, const void *v2)
{
    const Record *p1 = (Record *)v1;
    const Record *p2 = (Record *)v2;
    if (p1->id < p2->id)
        return -1;
    else if (p1->id > p2->id)
        return +1;
    else
        return 0;
}

// Print out the records
void print_records(Record *records, int num_of_recs)
{
    int i;
    for (i = 0; i < num_of_recs; i++)
    {
        printf("Record id: %d,   ", records[i].id);
        printf("Record value: %d \n", records[i].value);
    }
}

/* Precondition of this function:
    1. The two arrays to be merged are already sorted.
    2. The length of the two arrays are the same.
 */
Record *merge_sorted_records_arrays(Record *rec_arr_1, Record *rec_arr_2, int num_of_recs_per_arr, Record *merged_rec_arr)
{
    merged_rec_arr = malloc(2 * num_of_recs_per_arr * sizeof(Record));

    int i = 0, j = 0, k = 0;

    while (i < num_of_recs_per_arr && j < num_of_recs_per_arr)
    {
        if (rec_arr_1[i].id < rec_arr_2[j].id)
        {
            merged_rec_arr[k].id = rec_arr_1[i].id;
            merged_rec_arr[k].value = rec_arr_1[i].value;
            k = k + 1;
            i = i + 1;
        }
        else
        {
            merged_rec_arr[k].id = rec_arr_2[j].id;
            merged_rec_arr[k].value = rec_arr_2[j].value;
            k = k + 1;
            j = j + 1;
        }
    }

    while (i < num_of_recs_per_arr)
    {
        merged_rec_arr[k].id = rec_arr_1[i].id;
        merged_rec_arr[k].value = rec_arr_1[i].value;
        k = k + 1;
        i = i + 1;
    }

    while (j < num_of_recs_per_arr)
    {
        merged_rec_arr[k].id = rec_arr_2[j].id;
        merged_rec_arr[k].value = rec_arr_2[j].value;
        k = k + 1;
        j = j + 1;
    }

    return merged_rec_arr;
}

int main(int argc, char **argv)
{
    /* ================== Start up ================== */

    MPI_Init(&argc, &argv);

    // Determine the rank of the calling process in the communicator
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Determine the size of the group associated with a communicator
    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Get the name of the processor
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);

    // Sanity check
    printf("Hello from processor %s \n", processor_name);

    /* ================== The workload ================== */

    Record *recv_buf = malloc(size * NUM_OF_RECORDS * sizeof(Record));
    Record *records = malloc(NUM_OF_RECORDS * sizeof(Record));

    int i;
    for (i = 0; i < NUM_OF_RECORDS; i++)
    {
        records[i].id = (i + 1) * 10 + rank;
        records[i].value = records[i].id * 100;
    }

    // printf("=========== Before SHUFFLING the records, RANK: %d =========== \n", rank);
    // print_records(records, NUM_OF_RECORDS);

    shuffle(records, NUM_OF_RECORDS);
    // printf("=========== After SHUFFLING the records, RANK: %d =========== \n", rank);
    // print_records(records, NUM_OF_RECORDS);

    qsort(records, NUM_OF_RECORDS, sizeof(Record), rec_cmp);
    // printf("=========== After SORTING the records, RANK: %d =========== \n", rank);
    // print_records(records, NUM_OF_RECORDS);

    int count = 2;
    int block_lengths[2] = {1, 1};
    MPI_Aint offsets[2];
    offsets[0] = offsetof(Record, id);
    offsets[1] = offsetof(Record, value);
    MPI_Datatype types[2] = {MPI_INT, MPI_INT};
    MPI_Datatype MPI_Record;

    MPI_Type_create_struct(count,
                           block_lengths,
                           offsets,
                           types,
                           &MPI_Record);
    MPI_Type_commit(&MPI_Record);

    MPI_Gather(records, NUM_OF_RECORDS, MPI_Record,
               recv_buf, NUM_OF_RECORDS, MPI_Record,
               ROOT_RANK,
               MPI_COMM_WORLD);

    if (rank == ROOT_RANK)
    {
        printf("Inside the root process, before sorting the received arrays \n");
        print_records(recv_buf, size * NUM_OF_RECORDS);

        // The code below using merge_sorted_records_arrays is valid iff there are 4 processes running.
        // Be sure to specify "-n 4" after mpirun

        // Merge sorted arrays of records from process 0 and 1
        Record *merged_01;
        merged_01 = merge_sorted_records_arrays(recv_buf, &recv_buf[NUM_OF_RECORDS], NUM_OF_RECORDS, merged_01);

        // Merge sorted arrays of records from process 2 and 3
        Record *merged_23;
        merged_23 = merge_sorted_records_arrays(&recv_buf[2 * NUM_OF_RECORDS], &recv_buf[3 * NUM_OF_RECORDS], NUM_OF_RECORDS, merged_23);

        // Merge all
        Record *merged_0123;
        merged_0123 = merge_sorted_records_arrays(merged_01, merged_23, 2 * NUM_OF_RECORDS, merged_0123);

        printf("Inside the root process, after sorting the received arrays (with extra space) \n");
        print_records(merged_0123, 4 * NUM_OF_RECORDS);

        free(merged_01);
        free(merged_23);
        free(merged_0123);

        // qsort(recv_buf, size * NUM_OF_RECORDS, sizeof(Record), rec_cmp);
        // printf("Inside the root process, after sorting the received arrays (in place) \n");
        // print_records(recv_buf, size * NUM_OF_RECORDS);
    }

    /* ================== Tear down ================== */

    free(recv_buf);
    free(records);

    MPI_Finalize();

    return 0;
}
