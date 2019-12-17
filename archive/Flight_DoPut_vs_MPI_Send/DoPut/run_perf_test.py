import subprocess
KILO = 1000

for num_of_records in range(100*KILO, 5000*KILO, 200*KILO):
    for _ in range(10):
        subprocess.call(["./arrow-flight-benchmark",
                            "--server_host", "tcn1177",
                            "--records_per_stream", str(num_of_records),
                            "--records_per_batch", str(16384),
                            "--num_streams", str(1),
                            "--num_threads", str(1),
                            "--test_put"
                            ])
