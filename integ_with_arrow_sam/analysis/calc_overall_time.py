import re


def convert_time(time_str):
    h, m, s = time_str.split(":")
    return float(h) * 60 ** 2 + float(m) * 60 + float(s)


if __name__ == "__main__":
    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")

    flights_scheduled_time_per_node = []
    finish_sorting_time_per_chromo = []

    for node in nodes:
        with open(node + "_flight_sender.log", "r") as f:
            lines = f.readlines()
            for line in lines:
                if "All Flights scheduled." in line:
                    match = re.search(r"\d{2}:\d{2}:\d{2}.\d{3}", line)
                    flights_scheduled_time_per_node.append(
                        convert_time(match.group()))

        with open(node + "_sorter.log", "r") as f:
            lines = f.readlines()
            for line in lines:
                if "finished sorting" in line:
                    match = re.search(r"\d{2}:\d{2}:\d{2}", line)
                    finish_sorting_time_per_chromo.append(
                        convert_time(match.group()))

    assert(len(flights_scheduled_time_per_node) == len(nodes))
    assert(len(finish_sorting_time_per_chromo) == 25)

    overall_duration = max(finish_sorting_time_per_chromo) - \
        min(flights_scheduled_time_per_node)
    print("{:.3f}".format(overall_duration))
