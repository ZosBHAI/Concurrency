from multiprocessing import Process, Queue, Manager
import time
from random import randint
import logging
logging.basicConfig(level=logging.INFO)

NUMBERS_QUEUE = Queue()
RANDINT_QUEUE = Queue()
LOGGER_QUEUE = Queue()

"""
Simple example that showcases below  things in Multiprocessing 
1. Split the script into tasks that can run in parallel.
2. Properly execute the methods and join them with the main process.
3. Manage the shared data(lists, queues, return values) among processes.
4. Manage logging from different processes.



"""
def generate_numbers_list():
    LOGGER_QUEUE.put("Number generation Started")
    for x in range(1, 21):
        NUMBERS_QUEUE.put(x)
        time.sleep(5)
    NUMBERS_QUEUE.put("end")


def add_random():
    LOGGER_QUEUE.put("Number addition Started")
    while True:
        num = randint(10, 20)
        if not NUMBERS_QUEUE.empty():
            x = NUMBERS_QUEUE.get()
            LOGGER_QUEUE.put(f"add_random - {x}")
            if x == "end":
                break
            RANDINT_QUEUE.put(x+num)
    RANDINT_QUEUE.put("end")
    time.sleep(2)


def numbers_list(final_list):
    LOGGER_QUEUE.put("list conversion Started")
    while True:
        if not RANDINT_QUEUE.empty():
            x = RANDINT_QUEUE.get()
            LOGGER_QUEUE.put(f"numbers_list - {x}")
            if x == "end":
                break
            LOGGER_QUEUE.put(x)
            LOGGER_QUEUE.put(f"appending - {x}")
            final_list.append(x)


def convert_to_set(final_list):
    LOGGER_QUEUE.put(f"converting to set")
    return set(final_list)


def print_logs():
    while True:
        if not LOGGER_QUEUE.empty():
            x = LOGGER_QUEUE.get()
            logging.info(x)
            if x == "end":
                break


if __name__ == "__main__":
    logger_process = Process(target=print_logs)
    logger_process.start()
    LOGGER_QUEUE.put("Script Started")
    shared_list = Manager().list()

    generate_numbers_process = Process(target=generate_numbers_list)
    add_random_process = Process(target=add_random)
    numbers_list_process = Process(target=numbers_list, args=(shared_list,))
    generate_numbers_process.start()
    add_random_process.start()
    numbers_list_process.start()
    generate_numbers_process.join()
    add_random_process.join()
    numbers_list_process.join()

    print(shared_list)

    final_set = convert_to_set(shared_list)
    LOGGER_QUEUE.put(final_set)
    LOGGER_QUEUE.put("end")