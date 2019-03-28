from api.openapi_weather import RealtimeKmaWeather
import threading
import time


class WeatherLogger(threading.Thread):
    def __init__(self, service_key, debug=False):
        threading.Thread.__init__(self)

        self.on = True
        self.running = False
        self.debug = debug

        self.rkw = RealtimeKmaWeather(service_key)

    def current_time_millis(self):
        return int(round(time.time() * 1000))

    def run(self):
        last_ms = self.current_time_millis()
        current_ms = 0

        print('logging start...')
        self.rkw.log()
        print('logging end.')

        while self.on:
            current_ms = self.current_time_millis()

            if self.debug and \
                    ((current_ms - last_ms) / 1000 - int((current_ms - last_ms) / 1000)) == 0:
                print('debug> sec:', (current_ms - last_ms) / 1000)

            if current_ms - last_ms > 3600000 * 3:  # 3시간 간격
                last_ms = current_ms
                print('logging start...')
                self.rkw.log()
                print('logging end.')

            time.sleep(0.001)

    def start_logging(self):
        if self.debug:
            print('start logging weather from KMA Open API.')
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
    key = '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D'
    wl = WeatherLogger(key)
    wl.start_logging()

    try:
        if input('press <ENTER> key to stop logging...\n'):
            wl.stop()
    except KeyboardInterrupt:
        wl.stop()


