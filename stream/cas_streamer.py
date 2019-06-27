from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from entries.cas_entry import CasEntry
# from kafka import KafkaProducer
import time


class MyEventHandler(FileSystemEventHandler):
    kafka_producer = None

    def __init__(self, observer, filename):
        self.observer = observer
        self.filename = filename
        # self.kafka_producer = producer

    def on_created(self, event):
        # TODO: send nl entity to server
        if event.event_type == 'created':
            time.sleep(1)  # for waiting for creating completed
            entity = CasEntry(event.src_path)
            print('send>>', entity.get_category(category='all'))
            self.kafka_producer.send('natural_light_entity', b'send entity')
        else:
            pass


class CasEntryStreamer:
    def __init__(self, path):
        self.remote_dirpath = path

    def start_streaming(self):
        # producer = KafkaProducer(bootstrap_servers='localhost:9092')

        observer = Observer()
        event_handler = MyEventHandler(observer, self.remote_dirpath)
        observer.schedule(event_handler, self.remote_dirpath, recursive=True)
        observer.start()
        print('watchdog started.')
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()


if __name__ == "__main__":
    CasEntryStreamer('C:/Users/WitLab-DaeHwan/Desktop/tmp').start_streaming()

