import random

ints = [i for i in range(64 * 10 ** 6)]

random.shuffle(ints)

with open('ints.txt', 'a') as f:
    for i in range(len(ints)):
        f.write(str(ints[i])+'\n')
