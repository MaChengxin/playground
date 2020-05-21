from multiprocessing import Process

import numpy as np
import pandas as pd


def sort_df(df):
    df.sort_values(by=["b"], inplace=True)
    print(df)


if __name__ == "__main__":
    df = pd.DataFrame(np.random.rand(20, 3), columns=["a", "b", "c"])
    gb = df.groupby(pd.cut(df["b"], 4))
    # copy() is to suppress SettingWithCopyWarning
    partitioned_dfs = [gb.get_group(g).copy() for g in gb.groups]

    procs = []
    for df in partitioned_dfs:
        proc = Process(target=sort_df, args=(df,))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
