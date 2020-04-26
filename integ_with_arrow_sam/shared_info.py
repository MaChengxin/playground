SAM_FIELDS = ("QNAME", "FLAG", "RNAME", "POS", "MAPQ", "CIGAR",
              "RNEXT", "PNEXT", "TLEN", "SEQ", "QUAL", "OPTIONAL")

VALID_CHROMO_NAMES = ("chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9",
                      "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17",
                      "chr18", "chr19", "chr20", "chr21", "chr22", "chrM", "chrX", "chrY")

# This dict describes how the workload should be distributed given different number of nodes.
# TODO: complete this distribution dict, and make the work more balanced
WORKLOAD_DISTRIBUTION = {1: (VALID_CHROMO_NAMES,),  # the comma after VALID_CHROMO_NAMES is NEEDED!
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
