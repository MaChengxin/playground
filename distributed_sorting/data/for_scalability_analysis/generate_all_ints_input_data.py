from datetime import datetime
import numpy as np
import os
import random

NUM_OF_GROUPS = 100
NUM_RECORDS_PER_GROUP = np.lcm.reduce([i*2 for i in range(1, 17)])  # 1441440


def write_records_to_file(records, filename):
    with open(filename, "a") as f:
        for record in records:
            f.write(str(record[0]) + "\t" + str(record[1]) + "\n")
    print("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
          "]: finished writing to " + filename)


records = [(int(str(g)+str(i).zfill(7)), int(str(g).zfill(2)+str(i).zfill(14)))
           for g in range(NUM_OF_GROUPS) for i in range(NUM_RECORDS_PER_GROUP)]

for n in [1, 2, 4, 6, 8, 10, 12]:
    folder_name = str(n)+"_nodes"
    os.makedirs(folder_name)
    records_per_node = int(NUM_OF_GROUPS*NUM_RECORDS_PER_GROUP/n)
    for i in range(n):
        print("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
              "]: started writing to expected_records_on_node_"+str(i)+".txt")
        write_records_to_file(records[records_per_node*i:records_per_node*(i+1)],
                              folder_name+"/expected_records_on_node_"+str(i)+".txt")

random.shuffle(records)

for n in [1, 2, 4, 6, 8, 10, 12]:
    folder_name = str(n)+"_nodes"
    records_per_node = int(NUM_OF_GROUPS*NUM_RECORDS_PER_GROUP/n)
    for i in range(n):
        print("[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") +
              "]: started writing to records_on_node_"+str(i)+".txt")
        write_records_to_file(records[records_per_node*i:records_per_node*(i+1)],
                              folder_name+"/records_on_node_"+str(i)+".txt")
