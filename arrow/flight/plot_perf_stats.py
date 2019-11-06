import pandas as pd
import matplotlib.pyplot as plt

if __name__ == "__main__":
    perf_stats = pd.read_csv("perf_stats.csv", sep="\t",  header=None)
    perf_stats.plot(x=4, y=5)
    plt.show()
