import psutil
import time


def print_mem_usage(event):
    mem_usage = "{:.2f}".format(psutil.virtual_memory().available / (1024 ** 3))
    print(event + ", available memory: " + mem_usage + " GB")


if __name__ == "__main__":
    print_mem_usage("Before killing the Plasma Store")

    for proc in psutil.process_iter():
        # Note it is not "plasma_store"
        if "plasma-store" in proc.name():
            proc.kill()
            print("Plasma Store process killed.")

    time.sleep(5)  # wait for a while to see the effect of killing the process
    print_mem_usage("After killing the Plasma Store")
