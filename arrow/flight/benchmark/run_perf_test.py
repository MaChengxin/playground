import subprocess
KILO = 1000

for parallel_factor in range(1, 9):
    for num_of_records in range(100*KILO, 1100*KILO, 100*KILO):
        for _ in range(10):
            subprocess.call(["./arrow-flight-benchmark",
                             "--server_host", "172.31.11.18",
                             "--records_per_stream", str(num_of_records),
                             "--num_streams", str(parallel_factor),
                             "--num_threads", str(parallel_factor)
                             ])
