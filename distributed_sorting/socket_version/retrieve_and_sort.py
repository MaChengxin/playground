from datetime import datetime
import os
import pandas as pd
import socket
import sys


if __name__ == "__main__":
    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("started unpickling the records from pkl files\n")

    unpickled_dfs = []
    with os.scandir(socket.gethostname()+'/recved/') as pkl_files:
        for pkl_file in pkl_files:
            unpickled_dfs.append(pd.read_pickle(socket.gethostname()+'/recved/'+pkl_file.name))

    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write(
            "finished unpickling the records from pkl files, started concatenating\n")

    all_records = pd.concat(unpickled_dfs)

    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished concatenating, started sorting the records\n")

    all_records.sort_values(by=['group_name', 'seq'], inplace=True)

    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished sorting the records, started writing to csv\n")

    all_records.reset_index(drop=True, inplace=True)
    all_records.to_csv(socket.gethostname()+'.csv', sep='\t', index=False)

    with open(socket.gethostname()+'_r.log', 'a') as f:
        f.write('[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']: ')
        f.write("finished writing to csv \n")
