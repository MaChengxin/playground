import pandas as pd
import matplotlib.pyplot as plt

col_names = ["Test type (0: Get; 1: Put)",
             "Number of streams",
             "Number of threads",
             "Number of records per stream",
             "Number of records per batch",
             "Data transferred from the client to the server (MB)",
             "Time elapsed (s)",
             "Speed (MB/s)"]

if __name__ == "__main__":
    perf_stats = pd.read_csv("perf_stats_1_1.csv",
                             sep="\t",
                             names=col_names)

    speeds = perf_stats[["Number of records per stream", "Speed (MB/s)"]]
    ax = speeds.boxplot(by=["Number of records per stream"])
    ax.grid(False)
    ax.set_xticklabels(speeds["Number of records per stream"].unique(),
                       rotation=30)
    ax.title.set_text("(DoPut, number of streams: 1, using tcn1177 as remote server)")
    ax.set_xlabel("Number of records per stream")
    ax.set_ylabel("Speed (MB/s)")
    plt.show()
