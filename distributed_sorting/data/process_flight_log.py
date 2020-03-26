import matplotlib.pyplot as plt
import numpy as np
import os
import re


def convert_time(time_str):
    h, m, s = time_str.split(":")
    return int(h) * 60 ** 2 + int(m) * 60 + int(s)


class Log:
    hhmmss = r"\d{2}:\d{2}:\d{2}"

    def __init__(self, node_name, folder):
        self.node_name = node_name
        self.moments = {"started reading input file": None,
                        "finished reading input file, started partitioning the records": None,
                        "finished partitioning the records, started putting them to Plasma": None,
                        "finished putting partitioned records to Plasma": None,
                        "send-to-dest started": None,
                        "putting received data to Plasma finished": None,
                        "started retrieving the records from Plasma": None,
                        "finished retrieving the records from Plasma, started converting to DataFrame": None,
                        "finished converting to DataFrame, started concatenating": None,
                        "finished concatenating, started sorting the records": None,
                        "finished sorting the records": None}
        self.parse_s_log(folder)
        self.parse_r_log(folder)
        self.execution_time = {"partitioning": None,
                               "serialization": None,
                               "communication": None,
                               "deserialization": None,
                               "merging": None,
                               "sorting": None}
        self.calculate_exec_time()

    def parse_s_log(self, folder):
        filename = self.node_name+".bullx_s.log"
        with open(folder+"/"+filename) as f:
            lines = f.readlines()

        assert len(lines) == 5

        moments = list()
        for line in lines:
            match = re.search(Log.hhmmss, line)
            moments.append(match.group())

        self.moments["started reading input file"] = moments[0]
        self.moments["finished reading input file, started partitioning the records"] = moments[1]
        self.moments["finished partitioning the records, started putting them to Plasma"] = moments[2]
        self.moments["finished putting partitioned records to Plasma"] = moments[3]
        self.moments["send-to-dest started"] = moments[4]

    def parse_r_log(self, folder):
        filename = self.node_name+".bullx_r.log"
        with open(folder+"/"+filename) as f:
            lines = f.readlines()

        moments = list()
        for line in lines:
            if "received data, started putting to Plasma" in line:
                # print("Skipped processing starting time of putting to Plasma")
                pass
            elif "putting received data to Plasma finished" in line:
                match = re.search(Log.hhmmss, line)
                t = match.group()
                # print("Updating finishing time of putting received data to Plasma:")
                # print(t)
            else:
                match = re.search(Log.hhmmss, line)
                moments.append(match.group())

        assert len(moments) == 6

        self.moments["putting received data to Plasma finished"] = t
        self.moments["started retrieving the records from Plasma"] = moments[0]
        self.moments["finished retrieving the records from Plasma, started converting to DataFrame"] = moments[1]
        self.moments["finished converting to DataFrame, started concatenating"] = moments[2]
        self.moments["finished concatenating, started sorting the records"] = moments[3]
        self.moments["finished sorting the records"] = moments[4]

    def calculate_exec_time(self):
        self.execution_time["partitioning"] = \
            convert_time(self.moments["finished partitioning the records, started putting them to Plasma"]) - \
            convert_time(self.moments["finished reading input file, started partitioning the records"])
        
        self.execution_time["serialization"] = \
            convert_time(self.moments["finished putting partitioned records to Plasma"]) - \
            convert_time(self.moments["finished partitioning the records, started putting them to Plasma"])
        
        self.execution_time["communication"] = \
            convert_time(self.moments["putting received data to Plasma finished"]) - \
            convert_time(self.moments["send-to-dest started"])
        
        self.execution_time["deserialization"] = \
            convert_time(self.moments["finished converting to DataFrame, started concatenating"]) - \
            convert_time(self.moments["started retrieving the records from Plasma"])
        
        self.execution_time["merging"] = \
            convert_time(self.moments["finished concatenating, started sorting the records"]) - \
            convert_time(self.moments["finished converting to DataFrame, started concatenating"])
        
        self.execution_time["sorting"] = \
            convert_time(self.moments["finished sorting the records"]) - \
            convert_time(self.moments["finished concatenating, started sorting the records"])


all_items = os.listdir()
all_folders = [i for i in all_items if os.path.isdir(i)]

stats_for_plot = dict()
    
for folder in all_folders:
    stats_for_plot[folder] = dict()

    all_files = os.listdir(folder)
    log_files = [f for f in all_files if ".log" in f]
    node_list = list(set([lf.split(".")[0] for lf in log_files]))

    logs = list()

    for node in node_list:
        logs.append(Log(node, folder))

    overall_duration = \
        max([convert_time(log.moments["finished sorting the records"]) for log in logs]) - \
        min([convert_time(log.moments["finished reading input file, started partitioning the records"])for log in logs])

    stats_for_plot[folder]["overall_duration"] = overall_duration

    overall_stats = {"partitioning": {"mean": None, "std": None},
                    "serialization": {"mean": None, "std": None},
                    "communication": {"mean": None, "std": None},
                    "deserialization": {"mean": None, "std": None},
                    "merging": {"mean": None, "std": None},
                    "sorting": {"mean": None, "std": None}}

    for key in overall_stats.keys():
        overall_stats[key]["mean"] = \
            np.mean([log.execution_time[key] for log in logs])
        overall_stats[key]["std"] = \
            np.std([log.execution_time[key] for log in logs])

    # for log in logs:
    #     print(log.node_name)
    #     print(log.execution_time)

    # print(overall_stats)

    stats_for_plot[folder]["overall_stats"] = overall_stats


keys = [k for k in stats_for_plot.keys()]
nums_nodes = sorted(keys, key=lambda s:int(s.split("_")[0]))

overall_durations = [stats_for_plot[num_nodes]["overall_duration"] for num_nodes in nums_nodes]
par_means = [stats_for_plot[num_nodes]["overall_stats"]["partitioning"]["mean"] for num_nodes in nums_nodes]
ser_means = [stats_for_plot[num_nodes]["overall_stats"]["serialization"]["mean"] for num_nodes in nums_nodes]
comm_means = [stats_for_plot[num_nodes]["overall_stats"]["communication"]["mean"] for num_nodes in nums_nodes]
deser_means = [stats_for_plot[num_nodes]["overall_stats"]["deserialization"]["mean"] for num_nodes in nums_nodes]
mer_means = [stats_for_plot[num_nodes]["overall_stats"]["merging"]["mean"] for num_nodes in nums_nodes]
sort_means = [stats_for_plot[num_nodes]["overall_stats"]["sorting"]["mean"] for num_nodes in nums_nodes]
par_std = [stats_for_plot[num_nodes]["overall_stats"]["partitioning"]["std"] for num_nodes in nums_nodes]
ser_std = [stats_for_plot[num_nodes]["overall_stats"]["serialization"]["std"] for num_nodes in nums_nodes]
comm_std = [stats_for_plot[num_nodes]["overall_stats"]["communication"]["std"] for num_nodes in nums_nodes]
deser_std = [stats_for_plot[num_nodes]["overall_stats"]["deserialization"]["std"] for num_nodes in nums_nodes]
mer_std = [stats_for_plot[num_nodes]["overall_stats"]["merging"]["std"] for num_nodes in nums_nodes]
sort_std = [stats_for_plot[num_nodes]["overall_stats"]["sorting"]["std"] for num_nodes in nums_nodes]

x = np.arange(len(nums_nodes))  # the label locations
width = 0.18  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, overall_durations, width, label="overall duration")
rects2 = ax.bar(x + width/2, sort_means, width, yerr=sort_std, label="sorting (avg)")
rects3 = ax.bar(x + width/2, mer_means, width, yerr=mer_std, label="merging (avg)", bottom=sort_means)
rects4 = ax.bar(x + width/2, deser_means, width, yerr=deser_std, label="deserialization (avg)", bottom=[sum(val) for val in zip(sort_means, mer_means)])
rects4 = ax.bar(x + width/2, comm_means, width, yerr=comm_std, label="communication (avg)", bottom=[sum(val) for val in zip(sort_means, mer_means, deser_means)])
rects6 = ax.bar(x + width/2, ser_means, width, yerr=ser_std, label="serialization (avg)", bottom=[sum(val) for val in zip(sort_means, mer_means, deser_means, comm_means)])
rects7 = ax.bar(x + width/2, par_means, width, yerr=par_std, label="partitioning (avg)", bottom=[sum(val) for val in zip(sort_means, mer_means, deser_means, comm_means, ser_means)])

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel("Time (s)")
# ax.set_title("")
# ax.set_xticks(x)
ax.set_xticks([])
# ax.set_xticklabels(nums_nodes)
ax.legend()

# Add a table at the bottom of the axes
# https://matplotlib.org/3.1.1/gallery/misc/table_demo.html#sphx-glr-gallery-misc-table-demo-py
average_durations = [sum(val) for val in zip(sort_means, mer_means, deser_means, comm_means, ser_means, par_means)]
overall_speedup = [round(72/t, 2) for t in overall_durations]
average_speedup = [round(72/t, 2) for t in average_durations]
theoretical_speedup = [round(72/t, 2) for t in sort_means]
overhead = [sum(val) for val in zip(mer_means, deser_means, comm_means, ser_means, par_means)]
overhead_vs_sorting = [round(o/s, 2) for o, s in zip(overhead, sort_means)]

plt.table(cellText=[overall_speedup, average_speedup, theoretical_speedup, overhead_vs_sorting],
          rowLabels=["overall speedup", "average speedup", "theoretical speedup", "overhead vs sorting"],
          rowLoc="right",
          colLabels=nums_nodes,
          cellLoc="center",
          loc="bottom")

plt.subplots_adjust(left=0.265, right=0.99, bottom=0.19, top=1.0)
plt.savefig("scalability_all_ints.pdf")
# plt.show()
