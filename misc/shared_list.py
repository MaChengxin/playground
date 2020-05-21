""" Revised from https://stackoverflow.com/a/42490484/5723556 """

from multiprocessing import Process, Manager, Lock


def append_to_list(lock, shared_list, i):
    lock.acquire()
    try:
        shared_list.append(i)
        print(shared_list)
    finally:
        lock.release()
    if set(shared_list) == set([0, 1, 2, 3, 4]):
        print("Yeah")


if __name__ == "__main__":
    manager = Manager()
    shared_list = manager.list()
    lock = Lock()

    procs = []
    for i in range(5):
        p = Process(target=append_to_list, args=(lock, shared_list, i))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()
