"""
Server: receiver of the file
References:
    https://www.thepythoncode.com/article/send-receive-files-using-sockets-python
    https://stackoverflow.com/questions/1233222/python-multiprocessing-easy-way-to-implement-a-simple-counter
"""

from ctypes import c_int
from multiprocessing import Lock, Process, Value
import os
import socket
import tqdm


SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5001
BUFFER_SIZE = 4096  # receive 4096 bytes each time
SEPARATOR = "<SEPARATOR>"

num_recved_files = Value(c_int)  # defaults to 0
lock = Lock()


def increment_num_recved_files():
    with lock:
        num_recved_files.value += 1


def receive_file(conn, address):
    # receive the file infos
    received = conn.recv(BUFFER_SIZE).decode()
    filename, filesize = received.split(SEPARATOR)
    # remove absolute path if there is
    filename = os.path.basename(filename)
    # convert to integer
    filesize = int(filesize)

    # start receiving the file from the socket and writing to the file stream
    progress = tqdm.tqdm(range(
        filesize), f"Receiving {filename}", unit="B", unit_scale=True, unit_divisor=1024)
    with open(filename, "wb") as f:
        for _ in progress:
            # read 1024 bytes from the socket (receive)
            bytes_read = conn.recv(BUFFER_SIZE)
            if not bytes_read:
                # nothing is received, file transmitting is done
                break
            # write to the file the bytes we just received
            f.write(bytes_read)
            # update the progress bar
            progress.update(len(bytes_read))

    # close the connection socket
    conn.close()
    print(f"[-] {address} is disconnected.")
    increment_num_recved_files()
    print(f"Number of received files: {num_recved_files.value}")


if __name__ == "__main__":
    # create the listening socket
    s = socket.socket()

    # bind the socket to the local address
    s.bind((SERVER_HOST, SERVER_PORT))

    # enabling the server to accept connections
    s.listen()
    print(f"[*] Listening as {SERVER_HOST}:{SERVER_PORT}")

    procs = []
    to_be_recved_file_counter = 0
    while to_be_recved_file_counter < 4:  # TODO: change this hard-coded number
        # accept connection if there is any
        conn, address = s.accept()
        print(f"[+] {address} is connected.")

        to_be_recved_file_counter += 1
        print(f"Counter of file to be received: {to_be_recved_file_counter}")

        proc = Process(target=receive_file, args=(conn, address))
        procs.append(proc)
        proc.daemon = True
        proc.start()

    for proc in procs:
        proc.join()

    print(f"Received all data")
