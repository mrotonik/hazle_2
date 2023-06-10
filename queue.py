import hazelcast


def handle_queue():
    # Connect to Hazelcast cluster
    client = hazelcast.HazelcastClient()

    # called 'my-queue'
    queue = client.get_queue("my-queue").blocking()

    for i in range(1000):
        try:
            queue.put(f'value{i}')
            print('All ok')
        except :
            print("Queue is full!")


    client.shutdown()

if __name__ == "__main__":
    handle_queue()