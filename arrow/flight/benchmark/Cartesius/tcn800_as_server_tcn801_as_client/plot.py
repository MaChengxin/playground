import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

col_names = ["Test type (0: Get; 1: Put)",
             "Number of streams",
             "Number of threads",
             "Number of records per stream",
             "Number of records per batch",
             "Data transferred from the server to the client (MB)",
             "Time elapsed (s)",
             "Speed (MB/s)"]

if __name__ == "__main__":
    for i in range(8):
        perf_stats = pd.read_csv("perf_stats_0_{}.csv".format(i + 1),
                                 sep="\t",
                                 names=col_names)

        speeds = perf_stats[["Number of records per stream", "Speed (MB/s)"]]
        ax = speeds.boxplot(by=["Number of records per stream"])
        ax.grid(False)
        ax.set_xticklabels(speeds["Number of records per stream"].unique(),
                           rotation=30)
        ax.title.set_text("(DoGet, number of streams: {}, using remote server)".format(i + 1))
        ax.set_xlabel("Number of records per stream")
        ax.set_ylabel("Speed (MB/s)")
        plt.savefig("perf_stats_0_{}_DoGet.png".format(i + 1), dpi=300)
