import collections

# This dict describes how the workload should be distributed given different number of nodes.
WORKLOAD_DISTRIBUTION_LOOKUP = {1: (("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
                                     "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                                     "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM"),),
                                2: (("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8"),
                                    ("chr9", "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                                     "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM")),
                                4: (("chr1", "chr4", "chr5", "chr8", "chrM"),
                                    ("chr2", "chr3", "chr6", "chr7"),
                                    ("chr9", "chr10", "chr11",
                                     "chr12", "chr13", "chr14"),
                                    ("chr15", "chr16", "chr17", "chr18", "chr19",
                                     "chr20", "chr21", "chr22", "chrX", "chrY")),
                                6: (("chr1", "chr7", "chr15"),
                                    ("chr2", "chr6", "chr14"),
                                    ("chr4", "chr5", "chr9", "chrM"),
                                    ("chr3", "chr8", "chr13", "chr20"),
                                    ("chr11", "chr12", "chr18",
                                     "chr19", "chr22", "chrY"),
                                    ("chr10", "chr16", "chr17", "chr21", "chrX")),
                                8: (("chr1", "chr9"),
                                    ("chr4", "chr17", "chr21", "chr22", "chrM"),
                                    ("chr2", "chr8"),
                                    ("chr3", "chr5"),
                                    ("chr7", "chr13", "chr14"),
                                    ("chr10", "chr11", "chr12"),
                                    ("chr15", "chr16", "chr18", "chr19", "chrY"),
                                    ("chr6", "chr20", "chrX"))
                                }


def generate_object_id(chromo):
    id_prefix = "LOCALGENCHROMO"
    chr_id = convert_chromo_name(chromo)
    id_suffix = "PADD"
    return id_prefix + chr_id + id_suffix


def convert_chromo_name(chromo):
    chromo = chromo.strip("chr")
    if chromo == "X":
        return "23"
    elif chromo == "Y":
        return "24"
    elif chromo == "M":
        return "25"
    else:
        return chromo.zfill(2)


if __name__ == "__main__":
    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")

    chromo_groups = WORKLOAD_DISTRIBUTION_LOOKUP[len(nodes)]
    dispatch_plan = collections.defaultdict(dict)
    for i, group in enumerate(chromo_groups):
        for chromo in group:
            dispatch_plan[chromo]["destination"] = nodes[i]
            dispatch_plan[chromo]["object_id"] = generate_object_id(chromo)

    with open("dispatch_plan.txt", "w") as f:
        for chromo in dispatch_plan:
            f.write(chromo + ":" +
                    dispatch_plan[chromo]["destination"] + "," +
                    dispatch_plan[chromo]["object_id"] + "\n")
