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
    print('2017 Data Processing')
