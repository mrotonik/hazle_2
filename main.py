import hazelcast
from threading import Thread
import queue

# Distributed Map
def run_client(client_number, map):
    for i in range(1000):
        key = f"client-{client_number}-key-{i}"
        value = f"client-{client_number}-value-{i}"
        map.put(key, value)
        print(f"Client {client_number} added {key}: {value}")
        print(f"Map size after addition: {map.size().result()}")
    print(f"Client {client_number} finished")

# Bounded Queue
def producer():
    for i in range(200):
        print(f"Producer is producing item {i}")
        q.put(f"item {i}")
        print("Current queue size:", q.qsize())

def consumer(num):
    while True:
        item = q.get()
        print(f"Consumer {num} consumed {item}")
        print("Current queue size:", q.qsize())
        if item == "item 199":
            break

if __name__ == "__main__":
    client1 = hazelcast.HazelcastClient()
    client2 = hazelcast.HazelcastClient()
    client3 = hazelcast.HazelcastClient()

    map1 = client1.get_map("my-distributed-map").blocking()
    map2 = client2.get_map("my-distributed-map").blocking()
    map3 = client3.get_map("my-distributed-map").blocking()

    thread1 = Thread(target=run_client, args=(1, map1))
    thread2 = Thread(target=run_client, args=(2, map2))
    thread3 = Thread(target=run_client, args=(3, map3))

    thread1.start()
    thread2.start()
    thread3.start()


    thread1.join()
    thread2.join()
    thread3.join()


    entries = map1.entry_set()
    for key, value in entries:
        print(f"{key}: {value}")

    client1.shutdown()
    client2.shutdown()
    client3.shutdown()

    # Create a bounded queue
    q = queue.Queue(maxsize=100)

    # Create one producer and two consumers
    producer_thread = Thread(target=producer)
    consumer_thread1 = Thread(target=consumer, args=(1,))
    consumer_thread2 = Thread(target=consumer, args=(2,))

    producer_thread.start()
    consumer_thread1.start()
    consumer_thread2.start()

    producer_thread.join()
    consumer_thread1.join()
    consumer_thread2.join()

    print("All threads finished")
