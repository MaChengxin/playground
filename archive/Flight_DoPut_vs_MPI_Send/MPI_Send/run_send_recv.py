import subprocess
KILO = 1000

for num_of_records in range(100*KILO, 5000*KILO, 200*KILO):
    for _ in range(10):
        subprocess.call(["mpirun",
                         "--map-by", "node",
                         "--host", "tcn1177,tcn1187",
                         "--np", str(2),
                         "./send_recv",
                         str(num_of_records)
                         ])
