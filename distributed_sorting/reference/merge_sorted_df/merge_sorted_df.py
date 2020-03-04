import pandas as pd
from datetime import datetime

"""
Test run:
[2020-03-04 20:28:48]: start
[2020-03-04 20:29:08]: a_rec sorted
[2020-03-04 20:29:29]: b_rec sorted
[2020-03-04 20:29:49]: c_rec sorted
[2020-03-04 20:30:09]: d_rec sorted
[2020-03-04 20:30:13]: got abcd_concat_1
[2020-03-04 20:31:49]: got abcd_rec_sorted_1
[2020-03-04 20:32:03]: got abcd_concat_2
[2020-03-04 20:32:42]: got abcd_rec_sorted_2
"""


if __name__ == "__main__":
    a_rec = pd.read_csv("records_on_node_0.txt",
                        sep="\t",
                        names=["group_name", "seq", "data"])

    b_rec = pd.read_csv("records_on_node_1.txt",
                        sep="\t",
                        names=["group_name", "seq", "data"])

    c_rec = pd.read_csv("records_on_node_2.txt",
                        sep="\t",
                        names=["group_name", "seq", "data"])

    d_rec = pd.read_csv("records_on_node_3.txt",
                        sep="\t",
                        names=["group_name", "seq", "data"])

    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: start')

    a_rec_sorted = a_rec.sort_values(by=['group_name', 'seq'])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: a_rec sorted')

    b_rec_sorted = b_rec.sort_values(by=['group_name', 'seq'])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: b_rec sorted')

    c_rec_sorted = b_rec.sort_values(by=['group_name', 'seq'])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: c_rec sorted')

    d_rec_sorted = b_rec.sort_values(by=['group_name', 'seq'])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: d_rec sorted')

    abcd_concat_1 = pd.concat([a_rec, b_rec, c_rec, d_rec])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: got abcd_concat_1')
    abcd_rec_sorted_1 = abcd_concat_1.sort_values(by=['group_name', 'seq'])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: got abcd_rec_sorted_1')

    abcd_concat_2 = pd.concat([a_rec_sorted, b_rec_sorted, c_rec_sorted, d_rec_sorted])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: got abcd_concat_2')
    abcd_rec_sorted_2 = abcd_concat_2.sort_values(by=['group_name', 'seq'])
    print('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: got abcd_rec_sorted_2')
