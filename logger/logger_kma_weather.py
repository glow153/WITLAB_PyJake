from api.oapi_kma_localweather import RealtimeKmaWeather
from abs_class import AbsLogger
from debug_module import Log
import datetime
import time


class RealtimeKmaWeatherLogger(AbsLogger):
    def __init__(self, debug=False, **log_properties):
        key = '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D'
        self.tag = 'RealtimeKmaWeatherLogger'
        self.api = RealtimeKmaWeather(service_key=key, tag=self.tag, debug=True)
        super().__init__(self.api, tag=self.tag, interal=3600000*3, debug=debug, **log_properties)

    def run(self):  # 매 "발표시각"에 API 호출해서 데이터 받아오도록 수정해야함...
        dt_now = datetime.datetime.now()
        dt_next_base = self.api.get_last_basedt(dt_now) + datetime.timedelta(hours=3)

        # log once at start
        self._log()

        while self._on:
            next_base_str = dt_next_base.strftime('%Y-%m-%d %H:%M')
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

            # self._dbg.print_e('compare time str:', next_base_str, now_str)
            if next_base_str <= now_str:
                self._log()
                dt_next_base += datetime.timedelta(hours=3)
                Log.d(self.tag, 'next base time:', dt_next_base.strftime('%Y-%m-%d %H:%M'))

            time.sleep(0.5)


if __name__ == '__main__':
    logging_properties = {'db_type': ['hdfs'],
                          'mode': 'append',
                          'station': '충청남도 천안시서북구 부성동',
                          'term': 'hourly'}
    rkwl = RealtimeKmaWeatherLogger(debug=True, **logging_properties)
    rkwl.start_logging()

    try:
        if input('press <ENTER> key to stop logging...\n'):
            rkwl.stop()
    except KeyboardInterrupt:
        rkwl.stop()

