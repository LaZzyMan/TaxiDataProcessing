'''
File Format:
> yyyymmdd.zip
> > yyyymmdd/
> > > hh/ (00-23)
> > > > yyyymmddhh.txt

Data Format:
0: uid
1: timestamp
2: longitude
3: latitude
4: head direct
5: speed
6: weight(0 or >0)
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
hh_folders = []
dates = []


def get_year_fp():
    '''
    get the complete trajectory file pointer of year
    :return: pointer to file
    '''
    fp = open(DATA_PATH + '/TaxiData/2013/2013.dat', 'wb+')
    for day in dates:
        print('Reading %s file...' % day)
        file_name = DATA_PATH + '/TaxiData/2013/' + day + '.zip'
        folder_name = Unzip.un_zip(file_name) + '/' + day + '/'
        for hh in hh_folders:
            hh_path = folder_name + hh + '/' + day + hh + '.txt'
            hh_fp = open(hh_path, 'rb')
            fp.write(hh_fp.read())
            hh_fp.close()
        shutil.rmtree(DATA_PATH + '/TaxiData/2013/' + day)
    fp.seek(0)
    return fp


if __name__ == '__main__':
    print('2013 Data Processing running...')
    start = time.time()
    # init global vars
    # test data path
    # DATA_PATH = '../test'
    # DATA_PATH = os.path.abspath(os.path.dirname(__file__))
    DATA_PATH = '../..'
    hh_folders = ['{:0>2d}'.format(i) for i in range(24)]
    dates = ['20130501']
    # init instance of Spatial Unit
    su = SpatialUnit.SpatialUnit(DATA_PATH + '/TaxiData/SpatialUnit/TAZ2010.shp')
    # init db operator
    db = DBOperation.OpenTSDB(host='192.168.61.251', port=4242)
    # unzip and data format transform
    # generate daily data
    if os.path.exists(DATA_PATH + '/TaxiData/2013/2013.dat'):
        os.remove(DATA_PATH + '/TaxiData/2013/2013.dat')
    fp_year = get_year_fp()
    # detect trajectories for cabs
    # cabs = {'cab_id': [ weight, from_unit ]}
    print('Detecting ODs...')
    cabs = {}
    ods = []
    num_error_on = 0
    num_error_off = 0
    for line in (i.decode().strip().split(',') for i in fp_year):
        cab_id = line[0]
        timestamp = int(line[1])
        weight = int(line[6])
        # add new empty cab to track list
        if cab_id not in cabs.keys():
            if weight == 0:
                cabs[cab_id] = [0, 0]
            else:
                continue
        else:
            if weight == cabs[cab_id][0]:
                # condition have not changed
                continue
            elif weight == 0:
                # 1 -> 0 add an od and change the weight
                lon = float(line[2][:3] + '.' + line[2][3:])
                lat = float(line[3][:2] + '.' + line[3][2:])
                cabs[cab_id][0] = weight
                if cabs[cab_id][1] == 0:
                    continue
                to_unit = su.find_unit_by_point(lon=lon, lat=lat)
                if not to_unit == -1:
                    ods.append({
                        'metric': 'taxi.test.od',
                        'timestamp': timestamp,
                        'value': 1,
                        'tags': {
                            'from_unit': cabs[cab_id][1],
                            'to_unit': to_unit
                        }
                    })
                else:
                    print('Getting off location out of range: lon %f, lat %f' % (lon, lat))
                    num_error_off = num_error_off +1
                    continue
            else:
                # 0 -> 1 record from unit id and change the weight
                lon = float(line[2][:3] + '.' + line[2][3:])
                lat = float(line[3][:2] + '.' + line[3][2:])
                from_unit = su.find_unit_by_point(lon=lon, lat=lat)
                if not from_unit == -1:
                    cabs[cab_id] = [weight, from_unit]
                else:
                    print('Boarding location out of range: lon %f, lat %f' % (lon, lat))
                    num_error_on = num_error_on +1
                    cabs[cab_id] = [weight, 0]
                    continue
    fp_year.truncate()
    fp_year.close()
    os.remove(DATA_PATH + '/TaxiData/2013/2013.dat')
    # put ods to openTSDB
    print('Sending Data (%d)...' % len(ods))
    db.put(ods, batch_size=60)
    print('Finished! Time Usage: ' + str(time.time() - start))
    print('Correct: %d.' % len(ods))
    print('Boarding out og range: %d.' % num_error_on)
    print('Boarding out og range: %d.' % num_error_off)
