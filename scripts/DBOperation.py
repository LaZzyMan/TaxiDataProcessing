import requests
import json
import time


class OpenTSDB:
    def __init__(self, host, port):
        '''
        :param host: '192.168.61.251'
        :param port: 4242
        '''
        self.db_url = 'http://%s:%d' % (host, port)

    def put_one(self, metric, value, tags, timestamp):
        '''
        :param metric:
        :param value:
        :param tags:
        :param timestamp:
        :return: (num_success, num_failed)
        '''
        data_point = {
            'metric': metric,
            'timestamp': timestamp,
            'value': value,
            'tags': tags
        }
        r = requests.post(self.db_url + '/api/put?details',
                          json=data_point)
        print('%d success, %d failed' % (r.json()['success'], r.json()['failed']))
        return r.json()['success'], r.json()['failed']

    def put(self, data, batch_size):
        start_time = time.time()
        s = requests.Session()
        data_points = []
        i = 1
        success_num = 0
        failed_num = 0
        for item in data:
            data_points.append(item)
            if len(data_points) == batch_size:

                r = s.post(self.db_url + '/api/put?details',
                           json=data_points)
                print('%d batch, %d success, %d failed' % (i, r.json()['success'], r.json()['failed']))
                success_num = success_num + r.json()['success']
                failed_num = failed_num + r.json()['failed']
                data_points = []
                i = i + 1
        if len(data_points) > 0:
            r = s.post(self.db_url + '/api/put?details',
                       json=data_points)
            print('%d batch, %d success, %d failed' % (i, r.json()['success'], r.json()['failed']))
            success_num = success_num + r.json()['success']
            failed_num = failed_num + r.json()['failed']
        print('Finished: %d success, %d failed.' % (success_num, failed_num))
        print(time.time() - start_time)
        return success_num, failed_num


if __name__ == '__main__':
    # test
    db = OpenTSDB(host='192.168.61.251',
                  port=4242)
    db.put_one(metric='taxi.test.balabala',
               value=10,
               timestamp='1520399261',
               tags={
                   'a': 0,
                   'b': 0
               })
    data = []
    t = int(time.time()) - 100000
    for i in range(1000):
        data.append({
            'metric': 'taxi.test.balabala',
            'value': i,
            'timestamp': t + i,
            'tags': {
                'a': i,
                'b': i
            }
        })
    db.put(data=data, batch_size=60)
