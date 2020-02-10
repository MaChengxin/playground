import random

key_vals = [(str(i).zfill(8), 'V'+str(i).zfill(8)) for i in range(64 * 10 ** 6)]

random.shuffle(key_vals)

with open('key_vals.txt', 'a') as f:
    for i in range(len(key_vals)):
        f.write(key_vals[i][0]+'\t' + 
                key_vals[i][1]+'\n')
