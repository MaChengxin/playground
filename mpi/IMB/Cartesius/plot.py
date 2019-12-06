import pandas as pd
import matplotlib.pyplot as plt

stats_one_node = pd.read_csv("MPI_PingPong_on_one_node_stats.csv", delim_whitespace=True)
stats_two_nodes = pd.read_csv("MPI_PingPong_on_two_nodes_stats.csv", delim_whitespace=True)

plt.plot(stats_one_node['#bytes'], stats_one_node['Mbytes/sec'], label='on one node')
plt.plot(stats_two_nodes['#bytes'], stats_two_nodes['Mbytes/sec'], label='on two nodes')

plt.xscale('log', basex=2)

plt.title('Intel(R) MPI Benchmarks 2019 Update 3, MPI-1 part PingPong')
plt.xlabel('#bytes')
plt.ylabel('Mbytes/sec')

plt.legend()
plt.savefig("IMB_PingPong.png", dpi=300)
