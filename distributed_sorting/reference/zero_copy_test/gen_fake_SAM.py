import random

fake_SAM = [("QNAME."+str(i), i % 2048, "RNAME."+str(i % 24).zfill(2),
             i % 100000, 255, "CIGAR.37M", "=", i % 100000, 220,
             "GAATTTAAACATAAAAATCCTTAACAAAATATTAGC",
             "IIIIIIIIIIIIII>9=IBB@C0295795409/8,7")
            for i in range(32 * 10 ** 6)]

random.shuffle(fake_SAM)

with open('32m_fake_SAM.txt', 'a') as f:
    for i in range(len(fake_SAM)):
        f.write(str(fake_SAM[i][0])+'\t' +
                str(fake_SAM[i][1])+'\t' +
                str(fake_SAM[i][2])+'\t' +
                str(fake_SAM[i][3])+'\t' +
                str(fake_SAM[i][4])+'\t' +
                str(fake_SAM[i][5])+'\t' +
                str(fake_SAM[i][6])+'\t' +
                str(fake_SAM[i][7])+'\t' +
                str(fake_SAM[i][8])+'\t' +
                str(fake_SAM[i][9])+'\t' +
                str(fake_SAM[i][10])+'\n')
