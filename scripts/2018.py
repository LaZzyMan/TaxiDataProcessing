'''
File Format:
> yyyymmdd/
> > traj_3city_yyyymmdd.tar.gz & Decompress.jar
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
dates = []

if __name__ == '__main__':
    DATA_PATH = '../..'
    dates = ['20180414']
    print('2018 Data Processing')
