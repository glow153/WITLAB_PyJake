from api.oapi_airkorea_pm_realtime import RealtimeParticulateMatter
import threading
import time


class ParticulateMatterLogger(threading.Thread):
    def __init__(self, service_key, debug=False):
        threading.Thread.__init__(self)

        self.on = True
        self.running = False
        self.debug = debug

        self.rpm = RealtimeParticulateMatter(service_key, debug)

    def current_time_millis(self):
        return int(round(time.time() * 1000))

    def run(self):
        last_ms = self.current_time_millis()
        current_ms = 0

        print('logging start...')
        self.rpm.log(['hdfs', 'mysql'], term='hourly')
        print('logging end.')

        while self.on:
            current_ms = self.current_time_millis()

            # if (current_ms - last_ms) / 1000 - int((current_ms - last_ms) / 1000) == 0:
            #     self.rpm.debug_print('debug> sec:' + str((current_ms - last_ms) / 1000))

            if current_ms - last_ms > 3600000:
                last_ms = current_ms
                print('logging start...')
                self.rpm.log(['hdfs', 'mysql'], term='hourly')
                print('logging end.')

            time.sleep(0.001)

    def start_logging(self):
        if self.debug:
            print('start logging pm from Airkorea API.')
        if not self.on:
            self.on = True
        if not self.running:
            self.running = True

        self.start()

    def pause(self):
        self.running = False

    def resume(self):
        self.running = True

    def stop(self):  # can't restart
        self.on = False

    def is_running(self):
        return self.running


if __name__ == '__main__':
    key = 'zo2rUB1wM3I11GNZFDuB84l4C94PZjP6cEb4qEff%2B94h83%2Fihaj1JJS75%2Bm0uHdFCchJw7SyGE0HZgKiZDpq%2FA%3D%3D'
    logger_pm = ParticulateMatterLogger(service_key=key, debug=True)
    logger_pm.start_logging()

    try:
        if input('press <ENTER> key to stop logging...\n'):
            logger_pm.stop()
    except KeyboardInterrupt:
        logger_pm.stop()


