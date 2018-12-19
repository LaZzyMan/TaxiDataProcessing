'''
File Format:
> yyyymmdd.csv

Data Format:
0: id
1: O_timestamp
2: O_longitude
3: O_latitude
4: O_state
5: D_timestamp
6: D_longitude
7: D_latitude
8: D_state
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
    print('2014 Data Processing running...')
    # init global var
    DATES = ['20140301']
    DATA_PATH = '../..'
    start = time.time()
    # init spatial unit and db instance
    su = SpatialUnit.SpatialUnit(shp_path=DATA_PATH + '/TaxiData/SpatialUnit/TAZ2010.shp')
    db = DBOperation.OpenTSDB(host='192.168.61.251', port=4242)
    data_path = DATA_PATH + '/TaxiData/2014/'
    total_num_od = 0
    total_num_error_on = 0
    total_num_error_off = 0
    for day in DATES:
        print(day + ' running...')
        with open(data_path + day + '.csv', 'rb') as fp:
            # get total number of lines
            num_lines = sum(1 for x in fp)
            fp.seek(0)
            count = 0
            num_error_on = 0
            num_error_off = 0
            ods = []
            for line in (i.decode().strip().split(',') for i in fp):
                # Display current process
                if count % 1000 == 0:
                    print('%d / %d' % (count, num_lines))
                count += 1
                # skip the title of csv file
                if count == 1:
                    continue
                o_lon = float(line[2])
                o_lat = float(line[3])
                d_lon = float(line[6])
                d_lat = float(line[7])
                from_unit = su.find_unit_by_point(lon=o_lon, lat=o_lat)
                if from_unit == -1:
                    print('Boarding location out of range: lon %f, lat %f' % (o_lon, o_lat))
                    num_error_on += 1
                    continue
                to_unit = su.find_unit_by_point(lon=d_lon, lat=d_lat)
                if to_unit == -1:
                    print('Getting off location out of range: lon %f, lat %f' % (d_lon, d_lat))
                    num_error_off += 1
                    continue
                ods.append({
                    'metric': 'taxi.test.od',
                    'timestamp': int(float(line[5])),
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