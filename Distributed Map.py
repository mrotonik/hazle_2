import hazelcast

def fill_map():
    # Connect to Hazelcast cluster
    client = hazelcast.HazelcastClient()

    my_map = client.get_map("my-distributed-map").blocking()

    for i in range(1000):
        my_map.put(i, f'value{i}')

    client.shutdown()
    print('All ok')
if __name__ == "__main__":
    fill_map()
