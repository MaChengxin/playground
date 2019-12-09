import pandas as pd
import matplotlib.pyplot as plt

labels = []
csv_files = []

with open('history.log') as f:
    commands = f.readlines()
    for command in commands:
        mpirun_and_flags, log_file = command.split(" > ")
        labels.append(mpirun_and_flags[7:])
        csv_files.append(log_file.replace('log', 'csv').strip('\n'))

stats = []

for csv_file in csv_files:
    stats.append(pd.read_csv(csv_file, delim_whitespace=True))
    plt.plot(stats[-1]['#bytes'], stats[-1]['Mbytes/sec'], label=labels.pop(0))

plt.xscale('log', basex=2)

plt.title('Intel(R) MPI Benchmarks 2019 Update 3, MPI-1 part PingPong')
plt.xlabel('#bytes')
plt.ylabel('Mbytes/sec')

plt.legend()
plt.show()
