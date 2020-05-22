from pyarrow import plasma

if __name__ == "__main__":
    client = plasma.connect("/tmp/plasma")
    object_id = plasma.ObjectID(20 * b"a")
    object_size = 5 * 10 ** 9
    buffer = memoryview(client.create(object_id, object_size))
    for i in range(5 * 10 ** 9):
        buffer[i] = i % 128
    client.seal(object_id)
    client.disconnect()
