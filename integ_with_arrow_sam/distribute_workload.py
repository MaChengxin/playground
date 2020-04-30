# This dict describes how the workload should be distributed given different number of nodes.
# TODO: complete this distribution dict, and make the work more balanced
WORKLOAD_DISTRIBUTION_LOOKUP = {1: (("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
                                     "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                                     "chr18", "chr19", "chr20", "chr21", "chr22", "chrM", "chrX", "chrY"),),
                                2: (("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9"),
                                    ("chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                                     "chr18", "chr19", "chr20", "chr21", "chr22", "chrM", "chrX", "chrY")),
                                5: (("chr1", "chr2", "chr3"),
                                    ("chr4", "chr5", "chr6", "chr7",),
                                    ("chr8", "chr9", "chr10", "chr11", "chr12"),
                                    ("chr13", "chr14", "chr15",
                                     "chr16", "chr17", "chr18"),
                                    ("chr19", "chr20", "chr21", "chr22", "chrM", "chrX", "chrY"))
                                }

if __name__ == "__main__":
    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")
        num_of_nodes = len(nodes)

    chromo_destination = {}

    chromo_groups = WORKLOAD_DISTRIBUTION_LOOKUP[num_of_nodes]
    for i, group in enumerate(chromo_groups):
        for chromo in group:
            chromo_destination[chromo] = nodes[i]

    with open("chromo_destination.txt", "w") as f:
        for chromo in chromo_destination:
            f.write(chromo + ":" + chromo_destination[chromo] + "\n")
