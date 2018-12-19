'''
File Format:
> yyyymmdd.zip
> > yyyymmdd/
> > > yyyymmdd.txt

Data Format:
split: /t
0: id
1: hash_id
2: timestamp
3: 0
4: latitude
5: longitude
12[4]: weight(空车/重车)
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
import os
import shutil
import time

DATA_PATH = ''
dates = []


if __name__ == '__main__':
    print('2016 Data Processing running...')
    start = time.time()
    # init global vars
    # test data path
    # DATA_PATH = '../test'
    # DATA_PATH = os.path.abspath(os.path.dirname(__file__))
    DATA_PATH = '/home/NotFound/data'
    dates = ['20160501']
    # init instance of Spatial Unit
    su = SpatialUnit.SpatialUnit(DATA_PATH + '/TaxiData/SpatialUnit/TAZ2010.shp')
    # init db operator
    db = DBOperation.OpenTSDB(host='192.168.61.251', port=4242)
    total_num_od = 0
    total_num_error_on = 0
    total_num_error_off = 0
    # to track cabs between two days
    track_list = {}
    for day in dates:
        # unzip and data format transform
        # generate daily data
        print(day + ' running...')
        if os.path.exists(DATA_PATH + '/TaxiData/2016/' + day):
            shutil.rmtree(DATA_PATH + '/TaxiData/2016/' + day)
        print('Unzip file...')
        with open(Unzip.un_zip(DATA_PATH + '/TaxiData/2016/' + day + '.zip') + '/' + day + '.txt', 'r') as fp:
            # detect trajectories for cabs
            num_lines = sum(1 for _ in fp)
            # num_lines = 0
            fp.seek(0)
            count = 0
            num_error_on = 0
            num_error_off = 0
            current_cab = ''
            current_state = False
            current_from_unit = 0
            ods = []
            for line in (i.strip().split('\t') for i in fp):
                # Display process
                if count % 10000 == 0:
                    print('%d / %d, ods number: %d' % (count, num_lines, len(ods)))
                count += 1
                cab = line[0]
                if current_cab == cab:
                    # cab has not changed
                    state = '重车' in line[12]
                    if state is current_state:
                        # state has not changed
                        continue
                    elif state:
                        # state has changed
                        # 0->1
                        current_state = state
                        lon = float(line[5][:3] + '.' + line[5][3:])
                        lat = float(line[4][:2] + '.' + line[4][2:])
                        current_from_unit = su.find_unit_by_point(lon, lat)
                        if current_from_unit == -1:
                            print('Boarding location out of range :(')
                            num_error_on += 1
                    else:
                        # 1->0
                        current_state = state
                        if current_from_unit == -1:
                            # board point out of range
                            continue
                        lon = float(line[5][:3] + '.' + line[5][3:])
                        lat = float(line[4][:2] + '.' + line[4][2:])
                        to_unit = su.find_unit_by_point(lon, lat)
                        if to_unit == -1:
                            # get off point out of range
                            print('Getting off location out of range :(')
                            num_error_off = num_error_off + 1
                            continue
                        else:
                            ods.append({
                                'metric': 'taxi.test.od',
                                'timestamp': int(line[2]),
                                'value': 1,
                                'tags': {
                                    'from_unit': current_from_unit,
                                    'to_unit': to_unit
                                }
                            })
                else:
                    # cab has changed
                    if current_state:
                        # current track unfinished
                        track_list[current_cab] = current_from_unit
                    current_cab = cab
                    current_state = '重车' in line[12]
                    if current_state:
                        # new car initial state is 1
                        if current_cab in track_list.keys():
                            # continue latest track
                            current_from_unit = track_list[current_cab]
                        else:
                            # start a new track
                            lon = float(line[5][:3] + '.' + line[5][3:])
                            lat = float(line[4][:2] + '.' + line[4][2:])
                            current_from_unit = su.find_unit_by_point(lon, lat)
                        if current_from_unit == -1:
                            print('Boarding location out of range:(')
                            num_error_on += 1
                            continue
                    else:
                        current_from_unit = 0
            fp.close()
        shutil.rmtree(DATA_PATH + '/TaxiData/2016/' + day)
        # put ods to openTSDB
        print('Sending Data (%d)...' % len(ods))
        db.put(ods, batch_size=60)
        total_num_error_on += num_error_on
        total_num_error_off += num_error_off
        total_num_od += len(ods)
    print('Finished! Time Usage: ' + str(time.time() - start))
    print('Correct: %d.' % total_num_od)
    print('Boarding out of range: %d.' % total_num_error_on)
    print('Boarding out of range: %d.' % total_num_error_off)

