""" https://docs.python.org/3.7/library/multiprocessing.html#exchanging-objects-between-processes
    https://stackoverflow.com/questions/26514172/returning-large-objects-from-child-processes-in-python-multiprocessing
    https://stackoverflow.com/questions/925100/python-queue-multiprocessing-queue-how-they-behave/925241#925241
    https://stackoverflow.com/questions/7839786/efficient-python-to-python-ipc/7840047#7840047
"""


from multiprocessing import Process, Queue


def gen_lst(q, lst_len):
    """ Generate a list whose values are from 0 to lst_len-1,
    put it in the queue.
    """
    lst = [i for i in range(lst_len)]
    q.put(lst)


if __name__ == '__main__':
    procs = []
    queues = []
    for i in range(2):
        q = Queue()
        queues.append(q)
        # Increasing the length of the list would dramatically decrease the performance.
        proc = Process(target=gen_lst, args=(q, 20000))
        procs.append(proc)
        proc.daemon = True
        proc.start()
        print("A process is started.")

    for proc in procs:
        proc.join()
        print("A process is joined.")

    for q in queues:
        print(q.get())
