from shared_info import WORKLOAD_DISTRIBUTION

if __name__ == "__main__":
    with open("nodeslist.txt", "r") as f:
        nodes = f.readline().split(",")
        num_of_nodes = len(nodes)

    chromo_destination = {}

    chromo_groups = WORKLOAD_DISTRIBUTION[num_of_nodes]
    for i, group in enumerate(chromo_groups):
        for chromo in group:
            chromo_destination[chromo] = nodes[i]

    with open("chromo_destination.txt", "w") as f:
        for chromo in chromo_destination:
            f.write(chromo + ":" + chromo_destination[chromo] + "\n")
