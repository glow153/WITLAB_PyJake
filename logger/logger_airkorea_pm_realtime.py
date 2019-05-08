from api.oapi_airkorea_pm_realtime import RealtimeParticulateMatter
from abs_class import AbsLogger


class RealtimePmLogger(AbsLogger):
    def __init__(self, debug=False, **log_properties):
        key = 'zo2rUB1wM3I11GNZFDuB84l4C94PZjP6cEb4qEff%2B94h83%2Fihaj1JJS75%2Bm0uHdFCchJw7SyGE0HZgKiZDpq%2FA%3D%3D'
        self.api = RealtimeParticulateMatter(service_key=key, tag='RealtimePmLogger', debug=True)
        super().__init__(self.api, tag='RealtimePmLogger', debug=debug, **log_properties)


if __name__ == '__main__':
    logging_properties = {'db_type': ['hdfs', 'mysql'],
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

