import pandas as pd
import matplotlib.pyplot as plt

col_names = ["Number of records",
             "Speed (MB/s)"]

if __name__ == "__main__":
    perf_stats = pd.read_csv("MPI_send_recv_records_SEND.log",
                             sep="\t",
                             names=col_names)

    ax = perf_stats.boxplot(by=["Number of records"])
    ax.grid(False)
    ax.set_xticklabels(perf_stats["Number of records"].unique(), rotation=30)
    ax.title.set_text("(SEND, to remote host)")
    ax.set_xlabel("Number of records")
    ax.set_ylabel("Speed (MB/s)")
    plt.show()
