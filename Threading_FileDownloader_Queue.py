import os, sys, threading
import requests
import queue
import time


class FileDownloader:
    """Queue implementation - File Downloader"""
    class Item:
        """Job queue item class"""
        def __init__(self,filename,was_interrupted=False):
            self.filename = filename
            self.was_interrupted = was_interrupted # flag to denote if the job was interrupted due to some error

    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
        self.block_size = 1024
        self.thread_instance = list()   #list contain all the thread instances - usefull when creating daemon thread and join
        self.q = queue.Queue(maxsize=0)  # worker threads will pick download job from the queue


    def t_getfile(self, link, filename, session):

        """
        Threaded function that uses a semaphore
        to not instantiate too many threads
        """

        while not self.q.empty():
            item = self.q.get()
            try:
                if not item.was_interrupted:

                    filepath = os.path.join(os.getcwd() + '/Downloads/' + str(item.filename))
                    os.makedirs(os.path.dirname(filepath), exist_ok=True)


                    if not os.path.isfile(filepath):
                        self.download_new_file(link, filepath, session)
                    else:
                        print('else')
                        current_bytes = os.stat(filepath).st_size

                        headers = requests.head(link).headers

                        print(headers)
                        if 'content-length' not in headers:
                            print(f"server doesn't support content-length for {link}")
                            return

                        total_bytes = int(requests.head(link).headers['content-length'])
                        print(f"header {requests.head(link).headers}")

                        if current_bytes < total_bytes:
                            #
                            self.continue_file_download(link, filepath, session, current_bytes, total_bytes)
                            print(f"Current byte < total - remaining  {total_bytes - current_bytes}")
                        else:
                            print(f"already done: {filename}")

            except IOError:
                item.was_interrupted = True
                self.q.put(item)

            finally:
                self.q.task_done()


    def download_new_file(self, link, filepath, session):
        print(f"downloading: {filepath}")
        if session == None:
            try:
                session = requests.Session()
                print(id(session))
                request = session.get(link, headers=self.headers, timeout=30, stream=True)
                self.write_file(request, filepath, 'wb')
            except requests.exceptions.RequestException as e:
                print(e)
        else:
            print("session")
            request = session.get(link, stream=True)
            self.write_file(request, filepath, 'wb')

    def continue_file_download(self, link, filepath, current_bytes, total_bytes):
        print(f"resuming: {filepath}")
        range_header = self.headers.copy()
        range_header['Range'] = f"bytes={current_bytes}-{total_bytes}"

        try:
            request = requests.get(link, headers=range_header, timeout=30, stream=True)
            self.write_file(request, filepath, 'ab')
        except requests.exceptions.RequestException as e:
            print(e)

    def write_file(self, content, filepath, writemode):
        with open(filepath, writemode) as f:
            for chunk in content.iter_content(chunk_size=self.block_size):
                if chunk:
                    f.write(chunk)

        print(f"completed file {filepath}", end='\n')
        f.close()


    def start_downloading(self,urls=None,session=None):
        for url in urls:
            print(url)
            filename = os.path.basename(url)  #last portion will be  the file name
            thread = threading.Thread(target=self.t_getfile, args=(url, filename, session), daemon=True)
            self.q.put(self.Item(filename))
            self.thread_instance.append(thread)
            thread.start()

        self.q.join()
        print(f"Downloading of {len(urls)} completed")



if __name__ == '__main__':
    yellow_taxi_data = ['https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-01.csv',
                        'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-07.csv',
                        'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-06.csv']
    dl = FileDownloader()
    dl.start_downloading(yellow_taxi_data)
