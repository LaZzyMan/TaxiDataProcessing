'''
File Format:
> yyyymmdd/
> yyyymmdd_od.txt

Data Format:
every two line record an od trajectory
0: id
1: event
2: state 1(single line) 0(double line)
3: time yyyymmddhhMMss
4: longitude float
5: latitude float
6: head direct
7: speed
8: GPS state 0/1
'''

try:
    from . import Unzip
except ImportError:
    import Unzip
try:
    from . import SpatialUnit
except ImportError:
    import SpatialUnit
try:
    from . import DBOperation
except ImportError:
    import DBOperation
import time

DATA_PATH = ''
DATES = []

if __name__ == '__main__':
    print('2012 Data Processing running...')
    # init global var
    DATES = ['20121101']
    DATA_PATH = '../..'
    start = time.time()
    # init spatial unit and db instance
    su = SpatialUnit.SpatialUnit(shp_path=DATA_PATH + '/TaxiData/SpatialUnit/TAZ2010.shp')
    db = DBOperation.OpenTSDB(host='192.168.61.251', port=4242)
    data_path = DATA_PATH + '/TaxiData/2012/'
    total_num_od = 0
    total_num_error_on = 0
    total_num_error_off = 0
    for day in DATES:
        print(day + ' running...')
        with open(data_path + day + '/' + day + '_od.txt', 'r') as fp:
            # get total number of lines
            num_lines = sum(1 for x in fp) / 2
            fp.seek(0)
            count = 0
            num_error_on = 0
            num_error_off = 0
            ods = []
            from_unit = 0
            for line in (i.strip().split(',') for i in fp):
                # Display current process
                if count % 2000 == 0:
                    print('%d / %d, ods number: %d' % (count / 2, num_lines, len(ods)))
                count += 1
                if not count % 2 == 0:
                    # record the from unit
                    lon = float(line[4])
                    lat = float(line[5])
                    from_unit = su.find_unit_by_point(lon, lat)
                    if from_unit == -1:
                        print('Boarding location out of range: lon %f, lat %f' % (lon, lat))
                        num_error_on += 1
                        continue
                else:
                    # record to unit
                    if from_unit == -1:
                        continue
                    lon = float(line[4])
                    lat = float(line[5])
                    to_unit = su.find_unit_by_point(lon, lat)
                    if to_unit == -1:
                        print('Getting off location out of range: lon %f, lat %f' % (lon, lat))
                        num_error_off += 1
                        continue
                    ods.append({
                        'metric': 'taxi.test.od',
                        'timestamp': int(time.mktime(time.strptime(line[3], '%Y%m%d%H%M%S'))),
                        'value': 1,
                        'tags': {
                            'from_unit': from_unit,
                            'to_unit': to_unit
                        }
                    })
            # put ods to openTSDB
            print('Sending Data (%d)...' % len(ods))
            db.put(ods, batch_size=60)
            total_num_od += len(ods)
            total_num_error_on += num_error_on
            total_num_error_off += num_error_off
            fp.close()
        print('Finished! Time Usage: ' + str(time.time() - start))
        print('Correct: %d.' % total_num_od)
        print('Boarding out of range: %d.' % total_num_error_on)
        print('Boarding out of range: %d.' % total_num_error_off)