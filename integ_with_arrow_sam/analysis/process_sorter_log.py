import collections
import re

CHROMOS = ("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
           "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
           "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY", "chrM")


def convert_time(time_str):
    h, m, s = time_str.split(":")
    return float(h) * 60 ** 2 + float(m) * 60 + float(s)


if __name__ == "__main__":
    re_time = r"\d{2}:\d{2}:\d{2}"
    re_chromo_id = r"chr[0-9XYM]{1,}"
    prog = re.compile("(%s|%s)" % (re_time, re_chromo_id))

    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")

    per_chromo_timestamps = {}
    for node in nodes:
        per_chromo_timestamps[node] = collections.defaultdict(list)
        with open(node + "_sorter.log", "r") as f:
            lines = f.readlines()

        for line in lines:
            if "chr" in line:
                t, chromo_id = prog.findall(line)
                per_chromo_timestamps[node][chromo_id].append(convert_time(t))

    per_chromo_sorting_time = {}
    for node in nodes:
        for chromo_id in per_chromo_timestamps[node]:
            per_chromo_sorting_time[chromo_id] = {}
            per_chromo_sorting_time[chromo_id]["retrieve_from_plasma"] = per_chromo_timestamps[node][chromo_id][1] - \
                per_chromo_timestamps[node][chromo_id][0]
            per_chromo_sorting_time[chromo_id]["convert_RB_to_DF"] = per_chromo_timestamps[node][chromo_id][2] - \
                per_chromo_timestamps[node][chromo_id][1]
            per_chromo_sorting_time[chromo_id]["merge"] = per_chromo_timestamps[node][chromo_id][3] - \
                per_chromo_timestamps[node][chromo_id][2]
            per_chromo_sorting_time[chromo_id]["sort"] = per_chromo_timestamps[node][chromo_id][4] - \
                per_chromo_timestamps[node][chromo_id][3]
            per_chromo_sorting_time[chromo_id]["put_back_to_plasma"] = per_chromo_timestamps[node][chromo_id][5] - \
                per_chromo_timestamps[node][chromo_id][4]

    with open("sorting_time_N_nodes.txt", "w") as f:
        f.write("\tretrieve_from_plasma\tconvert_RB_to_DF\tmerge\tsort\tput_back_to_plasma\n")
        for chromo in CHROMOS:
            f.write(chromo + "\t" +
                    str(per_chromo_sorting_time[chromo]["retrieve_from_plasma"]) + "\t" +
                    str(per_chromo_sorting_time[chromo]["convert_RB_to_DF"]) + "\t" +
                    str(per_chromo_sorting_time[chromo]["merge"]) + "\t" +
                    str(per_chromo_sorting_time[chromo]["sort"]) + "\t" +
                    str(per_chromo_sorting_time[chromo]["put_back_to_plasma"]) + "\n")
