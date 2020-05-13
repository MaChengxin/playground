
import collections
import re

import matplotlib.pyplot as plt

CHROMOS = ("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
           "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
           "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM")


class Log:
    def __init__(self, node_id):
        self.node_id = node_id
        self.parse_sender_log()
        self.parse_receiver_log()

    def parse_sender_log(self):
        with open(self.node_id+"_flight_sender.log", "r") as f:
            lines = f.readlines()
        for line in lines:
            if "All Flights scheduled" in line:
                match = re.search(r"\d{2}:\d{2}:\d{2}.\d{3}", line)
                self.flights_departure_time = match.group()

    def parse_receiver_log(self):
        self.flights_arrival_time = {}
        with open(self.node_id+"_flight_receiver.log", "r") as f:
            lines = f.readlines()
        for line in lines:
            id_match = re.search(r"RECEIVEDCHROMO[0-9a-zA-Z]{6}$", line)
            time_match = re.search(r"\d{2}:\d{2}:\d{2}.\d{3}", line)
            self.flights_arrival_time[id_match.group()] = time_match.group()


def infer_chromo_from_plasma_id(plasma_id):
    chromo_idx = int(plasma_id[14:16])
    if 0 < chromo_idx < 23:
        return "chr"+str(chromo_idx)
    elif chromo_idx == 23:
        return "chrX"
    elif chromo_idx == 24:
        return "chrY"
    elif chromo_idx == 25:
        return "chrM"


def convert_time(time_str):
    h, m, s = time_str.split(":")
    return float(h) * 60 ** 2 + float(m) * 60 + float(s)


def calc_n2n_comm_time(start_time, dest, all_arrival_time, idx):
    arrival_time_wrt_source = []
    for chromo in all_arrival_time[dest]:
        arrival_time_wrt_source.append(all_arrival_time[dest][chromo][idx])
    end_time = max(arrival_time_wrt_source)
    
    return float("{:.3f}".format(end_time - start_time))

if __name__ == "__main__":
    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")

    """ Load log from files. """
    logs = []
    for node in nodes:
        logs.append(Log(node))

    """ Analyze the logs: transform logs """
    all_departure_time = {}
    all_arrival_time = {}
    for log in logs:
        all_departure_time[log.node_id] = convert_time(log.flights_departure_time)
        all_arrival_time[log.node_id] = collections.defaultdict(list)
        for plasma_id in log.flights_arrival_time:
            chromo_id = infer_chromo_from_plasma_id(plasma_id)
            all_arrival_time[log.node_id][chromo_id].append(convert_time(log.flights_arrival_time[plasma_id]))

    """ Analyze the logs: calculate all flights duration """
    """ Current implementation of Flight have no knowledge of where a coming Flight came from.
        We assume "First depart, first arrive".
        Therefore we sort all_departure_time_except_self and arrival_time.
        (Sorting arrival_time can be skipped, because all_arrival_time[node][chromo] is already in sorted order when parsing the receiver log.)
    """
    all_flights_duration = {}
    for node in all_arrival_time:
        all_departure_time_except_self = []
        for n in all_departure_time:
            if n != node:
                all_departure_time_except_self.append(all_departure_time[n])
        all_departure_time_except_self.sort()
        for chromo in all_arrival_time[node]:
            arrival_time = all_arrival_time[node][chromo]
            arrival_time.sort()
            all_flights_duration[chromo] = [float("{:.3f}".format(arr - dep))
                                            for arr, dep in zip(arrival_time, all_departure_time_except_self)]

    """ Analyze the logs: calculate node to node communication time """
    node_to_node_comm_time = collections.defaultdict(dict)
    for dest in nodes:
        all_departure_time_except_self = {source: all_departure_time[source] 
                                          for source in all_departure_time if source != dest}

        all_departure_time_except_self = {k: v 
                                          for k, v in sorted(all_departure_time_except_self.items(), key=lambda item: item[1])}
        
        for idx, source in enumerate(all_departure_time_except_self):
            start_time = all_departure_time_except_self[source]
            node_to_node_comm_time[source][dest] = calc_n2n_comm_time(start_time, dest, all_arrival_time, idx)

    with open("node_to_node_comm_time.txt", "w") as f:
        for node in nodes:
            f.write("\t" + node)
        f.write("\n")
        for source in nodes:
            f.write(source + "\t")
            for dest in nodes:
                if source == dest:
                    f.write("*\t")
                else:
                    f.write(str(node_to_node_comm_time[source][dest]) + "\t")
            f.write("\n")
   
    """ Code below is for plotting. """

    data = []
    for chromo in CHROMOS:
        data.append(all_flights_duration[chromo])
    fig, ax = plt.subplots()
    ax.set_title("Time spent on transferring SAM data (file size 30GB in total) among N nodes")
    ax.boxplot(data, labels=CHROMOS)
    ax.set_ylabel("Time (seconds)")

    plt.show()
