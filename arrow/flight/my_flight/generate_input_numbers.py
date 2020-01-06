import random

UPPER_LIMIT = 3000000
NUM_OF_NODES = 3
nums_per_node = int(UPPER_LIMIT/NUM_OF_NODES)

num_lst = [i for i in range(UPPER_LIMIT)]

random.shuffle(num_lst)

for i in range(NUM_OF_NODES):
    with open('nums_on_node_'+str(i)+'.txt', 'a') as f:
        for j in range(nums_per_node):
            f.write(str(num_lst[nums_per_node*i+j])+'\n')
