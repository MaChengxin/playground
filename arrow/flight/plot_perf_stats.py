import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

col_names = ["Number of streams",
             "Number of threads",
             "Number of records per stream",
             "Number of records per batch",
             "Data transferred from the server to the client (MB)",
             "Time elapsed (s)",
             "Speed (MB/s)"]

if __name__ == "__main__":
    perf_stats = []

    for i in range(8):
        perf_stats.append(pd.read_csv("perf_stats/perf_stats_0{}.csv".format(i + 1),
                                      sep="\t",
                                      names=col_names))

        perf_stats[i][col_names[4]] = \
            perf_stats[i][col_names[4]].apply(lambda x: x / 1024)
        perf_stats[i][col_names[-1]] = \
            perf_stats[i][col_names[-1]].apply(lambda x: x / 1024)
        perf_stats[i].rename(columns={col_names[4]:
                                      "Data transferred from the server to the client (GB)",
                                      col_names[-1]:
                                      "Speed (GB/s)"},
                             inplace=True)

    speeds = pd.DataFrame(
        [perf_stats[i]["Speed (GB/s)"].values for i in range(8)]).T
    speeds.columns = ["{}".format(i + 1) for i in range(8)]
    sns.boxplot(data=speeds).set(xlabel="Number of streams",
                                 ylabel="Speed (GB/s)",
                                 title="Speeds of data transfer with different number of streams")

    # ax = perf_stats[0].plot(x=2, y=5, label="# of streams: 1",
    #                         title=perf_stats[0].columns.values[5])

    # for i in range(1, 8):
    #     perf_stats[i].plot(x=2, y=5, ax=ax,
    #                        label="# of streams: {}".format(i + 1))

    plt.show()
