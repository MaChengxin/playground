import random

NUM_OF_NODES = 4
NUM_OF_GROUPS = 8
RECORDS_PER_GROUP = 8 * 10**6

records_per_node = int(NUM_OF_GROUPS*RECORDS_PER_GROUP/NUM_OF_NODES)

records = [('GROUP'+str(g).zfill(2), str(i).zfill(7), 'DATA'+str(g).zfill(2)+str(i).zfill(7))
           for g in range(NUM_OF_GROUPS) for i in range(RECORDS_PER_GROUP)]

random.shuffle(records)

for i in range(NUM_OF_NODES):
    with open('records_on_node_'+str(i)+'.txt', 'a') as f:
        for j in range(records_per_node):
            f.write(records[records_per_node*i+j][0]+'\t' +
                    records[records_per_node*i+j][1]+'\t' +
                    records[records_per_node*i+j][2]+'\n')
