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
        # use long session
        start_time = time.time()
        s = requests.Session()
        data_points = []
        count = 1
        success_num = 0
        failed_num = 0
        for item in data:
            data_points.append(item)
            if len(data_points) == batch_size:

                r = s.post(self.db_url + '/api/put?details',
                           json=data_points)
                print('%d batch, %d success, %d failed' % (count, r.json()['success'], r.json()['failed']))
                success_num = success_num + r.json()['success']
                failed_num = failed_num + r.json()['failed']
                data_points = []
                count = count + 1
        if len(data_points) > 0:
            r = s.post(self.db_url + '/api/put?details',
                       json=data_points)
            print('%d batch, %d success, %d failed' % (count, r.json()['success'], r.json()['failed']))
            success_num = success_num + r.json()['success']
            failed_num = failed_num + r.json()['failed']
        print('Finished: %d success, %d failed.' % (success_num, failed_num))
        print(time.time() - start_time)
        return success_num, failed_num

    def query(self, start, end, queries):
        '''
        :param start: timestamp
        :param end: timestamp
        :param queries: [Query]
        :return:
        '''
        r = requests.post(self.db_url + '/api/put?details',
                          json={
                              'start': start,
                              'end': end,
                              'queries': [query.query_json for query in queries]
                          })
        return r.json()


class Query:
    def __init__(self, metric, tags={}, agg=None, filters=None, downsample=None):
        '''
        :param metric:
        :param tags:
        :param agg: aggregator eg. 'sum'
        :param filters: [{'type', 'tagk', 'filter'}]
        :param downsample: 't-fun' rg.'1m-sum'
        '''
        self.query_json = {
            'metric': metric,
            'tags': tags
        }
        if agg is not None:
            self.query_json['aggregator'] = agg
        if filters is not None:
            self.query_json['filters'] = filters
        if downsample is not None:
            self.query_json['downsample'] = downsample


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
    d = []
    t = int(time.time()) - 100000
    for i in range(1000):
        d.append({
            'metric': 'taxi.test.balabala',
            'value': i,
            'timestamp': t + i,
            'tags': {
                'a': i,
                'b': i
            }
        })
    db.put(data=d, batch_size=60)
