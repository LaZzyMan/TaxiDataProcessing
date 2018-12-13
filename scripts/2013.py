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

DATA_PATH = ''
hh_folders = []
dates = []


def get_file(date):
    '''
    get the complete trajectory file of DATE
    :param date: yyyymmdd(str)
    :return: pointer to file
    '''
    file_name = DATA_PATH + '/TaxiData/2013/' + date + '.zip'
    Unzip.un_zip(file_name)


if __name__ == '__main__':
    print('2013 Data Processing...')
    DATA_PATH = os.path.abspath(os.path.dirname(__file__))
    hh_folders = ['{:0>2d}'.format(i) for i in range(24)]
    dates = ['20130501']
    for date in dates:
        get_file(date)

