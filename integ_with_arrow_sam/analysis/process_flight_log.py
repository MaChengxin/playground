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
        self.flights_departure_time ={}

        with open(self.node_id+"_flight_sender.log", "r") as f:
            lines = f.readlines()
        
        for line in lines:
            if "All Flights scheduled" in line:
                match = re.search(r"\d{2}:\d{2}:\d{2}.\d{3}", line)
                self.flights_scheduled_time = match.group()
            else:
                id_match = re.search(r"LOCALGENCHROMO[0-9]{2}PADD$", line)
                time_match = re.search(r"\d{2}:\d{2}:\d{2}.\d{3}", line)
                self.flights_departure_time[id_match.group()] = time_match.group()


    def parse_receiver_log(self):
        self.flights_arrival_time = collections.defaultdict(list)
        
        with open(self.node_id+"_flight_receiver.log", "r") as f:
            lines = f.readlines()
        
        for line in lines:
            id_match = re.search(r"RECEIVEDCHROMO[0-9a-zA-Z]{6}$", line)
            time_match = re.search(r"\d{2}:\d{2}:\d{2}.\d{3}", line)
            """ for each received RB we record two timestamps:
                 - time before putting it to Plasma
                 - time after putting it to Plasma
                 arrival has two steps: arrival at the dest node and arrival in Plasma
            """
            self.flights_arrival_time[id_match.group()].append(time_match.group())


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

if __name__ == "__main__":
    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")

    """ Load log from files. """
    logs = []
    for node in nodes:
        logs.append(Log(node))

    """ Analyze the logs: transform logs """
    all_flights_scheduled_time = []
    all_flights_departure_time = collections.defaultdict(list)
    all_arrival_at_dest_time = collections.defaultdict(list)
    all_put_to_plasma_time = collections.defaultdict(list)
    
    for log in logs:
        all_flights_scheduled_time.append(convert_time(log.flights_scheduled_time))
        min_flights_scheduled_time = min(all_flights_scheduled_time)

        for plasma_id in log.flights_departure_time:
            chromo_id = infer_chromo_from_plasma_id(plasma_id)
            all_flights_departure_time[chromo_id].append(convert_time(log.flights_departure_time[plasma_id]))
        all_flights_departure_time[chromo_id].sort()

        for plasma_id in log.flights_arrival_time:
            chromo_id = infer_chromo_from_plasma_id(plasma_id)
            all_arrival_at_dest_time[chromo_id].append(convert_time(log.flights_arrival_time[plasma_id][0]))
            all_put_to_plasma_time[chromo_id].append(convert_time(log.flights_arrival_time[plasma_id][1]))


    with open("per_chromo_transfer_time.txt", "w") as f:
        for chromo in CHROMOS:
            f.write(chromo)
            for t in all_flights_departure_time[chromo]:
                f.write("\t" + "{:.3f}".format(t-min_flights_scheduled_time))
            for t in all_arrival_at_dest_time[chromo]:
                f.write("\t" + "{:.3f}".format(t-min_flights_scheduled_time))
            for t in all_put_to_plasma_time[chromo]:
                f.write("\t" + "{:.3f}".format(t-min_flights_scheduled_time))
            f.write("\n")