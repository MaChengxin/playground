import random

NUM_OF_NODES = 8
NUM_OF_GROUPS = 64
RECORDS_PER_GROUP = 2 * 10**6

records_per_node = int(NUM_OF_GROUPS*RECORDS_PER_GROUP/NUM_OF_NODES)

records = [('GROUP'+str(g), str(i), 'DATA'+str(g)+str(i))
           for g in range(NUM_OF_GROUPS) for i in range(RECORDS_PER_GROUP)]

random.shuffle(records)

for i in range(NUM_OF_NODES):
    with open('records_on_node_'+str(i)+'.txt', 'a') as f:
        for j in range(records_per_node):
            f.write(records[records_per_node*i+j][0]+'\t' +
                    records[records_per_node*i+j][1]+'\t' +
                    records[records_per_node*i+j][2]+'\n')
