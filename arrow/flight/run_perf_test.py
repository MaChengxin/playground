import subprocess

for num_of_records in range(10000000, 510000000, 10000000):
    subprocess.call(["./arrow-flight-benchmark",
                     "--records_per_stream", str(num_of_records),
                     "--num_streams", str(8),
                     "--num_threads", str(8)
                     ])
