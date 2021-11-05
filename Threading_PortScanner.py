import threading
import queue
import time
import socket


def worker():
    while True:
        port = q.get()
        scan_port(target, port)
        q.task_done()


def scan_port(target, port):
    try:
        # create stream socket with a one second timeout and attempt a connection
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((target, int(port)))

        # disallow further sends and receives
        s.shutdown(socket.SHUT_RDWR)
        with thread_lock:
            print(f'[+] Port {port} on {target} is OPEN')
    except ConnectionRefusedError:
        pass
    finally:
        s.close()

if __name__ == "__main__":

    #Get the IP address of the machine
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    print(f"Scanning {hostname} with ip address {IPAddr}")
    target =IPAddr

    # Define a print lock
    thread_lock = threading.Lock()

    # Create our queue
    q = queue.Queue()

    # Define number of threads
    for r in range(100):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()

    # Start timer before sending tasks to the queue
    start_time = time.time()

    print('Creating a task request for each port\n')

    # Create a task request for each possible port to the worker
    for port in range(1, 65535):
        q.put(port)

    # block until all tasks are done
    q.join()

    print(f"\nAll workers completed their tasks after {round(time.time() - start_time, 2)} seconds")




