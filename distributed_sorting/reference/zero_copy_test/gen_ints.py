import random

ints = [(i, i % 2048, i % 24,
             i % 100000, 255, 37, 42, i % 100000,
             220, 1234567890, 9876543210)
            for i in range(32 * 10 ** 6)]

random.shuffle(ints)

with open('32m_ints.txt', 'a') as f:
    for i in range(len(ints)):
        f.write(str(ints[i][0])+'\t' +
                str(ints[i][1])+'\t' +
                str(ints[i][2])+'\t' +
                str(ints[i][3])+'\t' +
                str(ints[i][4])+'\t' +
                str(ints[i][5])+'\t' +
                str(ints[i][6])+'\t' +
                str(ints[i][7])+'\t' +
                str(ints[i][8])+'\t' +
                str(ints[i][9])+'\t' +
                str(ints[i][10])+'\n')
