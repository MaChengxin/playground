from datetime import datetime
import random

NUM_OF_NODES = 4
NUM_OF_GROUPS = 64
NUM_RECORDS_PER_GROUP = 2 * 10**6

records_per_node = int(NUM_OF_GROUPS*NUM_RECORDS_PER_GROUP/NUM_OF_NODES)

records = [(int(str(g)+str(i).zfill(7)), "DATA"+str(g).zfill(2)+str(i).zfill(7))
           for g in range(NUM_OF_GROUPS) for i in range(NUM_RECORDS_PER_GROUP)]

random.shuffle(records)


def write_records_to_file(records, filename):
    with open(filename, "a") as f:
        for record in records:
            f.write(str(record[0]) + "\t" + record[1] + "\n")
    print("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
          "]: finished writing to " + filename)


for i in range(NUM_OF_NODES):
    print("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
          "]: started writing to records_on_node_"+str(i)+".txt")
    write_records_to_file(records[records_per_node*i:records_per_node*(i+1)],
                          "records_on_node_"+str(i)+".txt")
