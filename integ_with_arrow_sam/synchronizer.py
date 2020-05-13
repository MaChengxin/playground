import socket
import subprocess
import time

if __name__ == "__main__":
    host_name = socket.gethostname().strip(".bullx")

    with open("nodelist.txt", "r") as f:
        nodes = f.readline().strip("\n").split(",")

    with open("nodes_ready_for_flight.txt", "r") as f:
        ready_nodes = [node.strip("\n") for node in f.readlines()]

    while len(ready_nodes) < len(nodes):
        with open("nodes_ready_for_flight.txt", "r") as f:
            ready_nodes = [node.strip("\n") for node in f.readlines()]

    subprocess.call("./flight-sender")
