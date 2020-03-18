import os
import re


def convert_time(time_str):
    h, m, s = time_str.split(":")
    return int(h) * 60 ** 2 + int(m) * 60 + int(s)


class Log:
    hhmmss = r"\d{2}:\d{2}:\d{2}"

    def __init__(self, node_name):
        self.node_name = node_name
        self.times = {"started reading input file": None,
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
        self.parse_s_log()
        self.parse_r_log()
        self.execution_time = {"partitioning": None,
                               "serialization": None,
                               "communication": None,
                               "deserialization": None,
                               "merging": None,
                               "sorting": None}
        self.calculate_exec_time()

    def parse_s_log(self):
        filename = self.node_name+".bullx_s.log"
        with open(filename) as f:
            lines = f.readlines()

        assert len(lines) == 5

        times = list()
        for line in lines:
            match = re.search(Log.hhmmss, line)
            times.append(match.group())

        self.times["started reading input file"] = times[0]
        self.times["finished reading input file, started partitioning the records"] = times[1]
        self.times["finished partitioning the records, started putting them to Plasma"] = times[2]
        self.times["finished putting partitioned records to Plasma"] = times[3]
        self.times["send-to-dest started"] = times[4]

    def parse_r_log(self):
        filename = self.node_name+".bullx_r.log"
        with open(filename) as f:
            lines = f.readlines()

        times = list()
        for line in lines:
            if "received data, started putting to Plasma" in line:
                print("Skipped processing starting time of putting to Plasma")
            elif "putting received data to Plasma finished" in line:
                match = re.search(Log.hhmmss, line)
                t = match.group()
                print("Updating finishing time of putting received data to Plasma:")
                print(t)
            else:
                match = re.search(Log.hhmmss, line)
                times.append(match.group())

        assert len(times) == 6

        self.times["putting received data to Plasma finished"] = t
        self.times["started retrieving the records from Plasma"] = times[0]
        self.times["finished retrieving the records from Plasma, started converting to DataFrame"] = times[1]
        self.times["finished converting to DataFrame, started concatenating"] = times[2]
        self.times["finished concatenating, started sorting the records"] = times[3]
        self.times["finished sorting the records"] = times[4]

    def calculate_exec_time(self):
        self.execution_time["partitioning"] = \
            convert_time(self.times["finished partitioning the records, started putting them to Plasma"]) - \
            convert_time(self.times["finished reading input file, started partitioning the records"])
        
        self.execution_time["serialization"] = \
            convert_time(self.times["finished putting partitioned records to Plasma"]) - \
            convert_time(self.times["finished partitioning the records, started putting them to Plasma"])
        
        self.execution_time["communication"] = \
            convert_time(self.times["putting received data to Plasma finished"]) - \
            convert_time(self.times["send-to-dest started"])
        
        self.execution_time["deserialization"] = \
            convert_time(self.times["finished converting to DataFrame, started concatenating"]) - \
            convert_time(self.times["started retrieving the records from Plasma"])
        
        self.execution_time["merging"] = \
            convert_time(self.times["finished concatenating, started sorting the records"]) - \
            convert_time(self.times["finished converting to DataFrame, started concatenating"])
        
        self.execution_time["sorting"] = \
            convert_time(self.times["finished sorting the records"]) - \
            convert_time(self.times["finished concatenating, started sorting the records"])


all_files = os.listdir()
log_files = [f for f in all_files if ".log" in f]
node_list = list(set([lf.split(".")[0] for lf in log_files]))

logs = list()

for node in node_list:
    logs.append(Log(node))
