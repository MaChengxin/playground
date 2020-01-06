import random

NUM_OF_NODES = 3
NUM_OF_GROUPS = 30
UPPER_LIMIT_PER_GROUP = 1000

nums_per_node = int(NUM_OF_GROUPS*UPPER_LIMIT_PER_GROUP/NUM_OF_NODES)

data_lst = [('GROUP'+str(g), str(i), 'DATA'+str(g)+str(i))
            for g in range(NUM_OF_GROUPS) for i in range(UPPER_LIMIT_PER_GROUP)]

random.shuffle(data_lst)

for i in range(NUM_OF_NODES):
    with open('nums_on_node_'+str(i)+'.txt', 'a') as f:
        for j in range(nums_per_node):
            f.write(data_lst[nums_per_node*i+j][0]+'\t' +
                    data_lst[nums_per_node*i+j][1]+'\t' +
                    data_lst[nums_per_node*i+j][2]+'\n')
