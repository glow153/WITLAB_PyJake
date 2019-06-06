from api.oapi_kma_localweather_oldver import RealtimeKmaWeather
from abs_class import AbsLogger


class RealtimeWeatherLogger(AbsLogger):
    def __init__(self, debug=False, **log_properties):
        key = '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D'
        self.api = RealtimeKmaWeather(service_key=key, tag='RealtimeWeatherLogger', debug=True)
        super().__init__(self.api, tag='RealtimeWeatherLogger', debug=debug, **log_properties)


if __name__ == '__main__':
    logging_properties = {'db_type': ['hdfs'],
                          'mode': 'append',
                          'station': 'cheonan_all',
                          'term': 'hourly'}
    rpl = RealtimePmLogger(debug=True, **logging_properties)
    rpl.start_logging()

    try:
        if input('press <ENTER> key to stop logging...\n'):
            rpl.stop()
    except KeyboardInterrupt:
        rpl.stop()

