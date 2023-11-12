import threading
import time
def int_sleep():
    for _ in range(1, 10):
        print("in the child thread")
        time.sleep(1)
        print("sleep")

def main1():
    print("in the main thread")
    thread = threading.Thread(target=int_sleep)
    #thread.daemon = True
    thread.start()
    time.sleep(2)
    print("main thread end...")


if __name__ == '__main__':
    print("in the main thread")
    thread = threading.Thread(target=int_sleep)
    thread.daemon = True
    thread.start()
    time.sleep(2)
    #thread.join()
    print("main thread end...")
    #main1()
