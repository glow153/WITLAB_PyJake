from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from entries.cas_entry import CasEntity
from kafka import KafkaProducer
import time


class MyEventHandler(FileSystemEventHandler):
    kafka_producer = None

    def __init__(self, observer, filename, producer):
        self.observer = observer
        self.filename = filename
        self.kafka_producer = producer

    def on_created(self, event):
        # TODO: send nl entity to server
        if event.event_type == 'created':
            time.sleep(1)  # for waiting for creating completed
            entity = CasEntity(event.src_path)
            print('send>>', entity.get_category())
            self.kafka_producer.send('natural_light_entity', b'send entity')
        else:
            pass


class NLEntityStreamer:
    def __init__(self):
        self.remote_dirpath = '/home/witlab/tmp'

    def start_streaming(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        observer = Observer()
        event_handler = MyEventHandler(observer, self.remote_dirpath, producer)
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
    NLEntityStreamer().start_streaming()

