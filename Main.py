from SSHConnect import SSHConnect
import sys
from config import HOST, USERNAME, PASSWORD, DATA_PATH


if __name__ == '__main__':

    y_list = []
    # check for year params
    try:
        _ = sys.argv[1]
        y_list = sys.argv[1:]
    except IndexError as _:
        print('ERROR: No Year Param.')
        exit()
    ssh = SSHConnect(host=HOST,
                     username=USERNAME,
                     password=PASSWORD)
    # upload tools scripts
    ssh.upload_file(local_file='scripts/SpatialUnit.py',
                    remote_path=DATA_PATH + 'SpatialUnit.py')
    ssh.upload_file(local_file='scripts/DBOperation.py',
                    remote_path=DATA_PATH + 'DBOperation.py')
    ssh.upload_file(local_file='scripts/Unzip.py',
                    remote_path=DATA_PATH + 'Unzip.py')
    # exec main script
    for year in y_list:
        ssh.connect()
        ssh.upload_file(local_file='scripts/%s.py' % year,
                        remote_path=DATA_PATH + '%s.py' % year)
        ssh.exec_cmd('python ' + DATA_PATH + '%s.py' % year)
        print('Script Executed :)')
        ssh.exec_cmd('rm ' + DATA_PATH + '%s.py' % year)
        print('Script Removed :)')
        ssh.close()
    # remove tools scripts
    ssh.connect()
    ssh.exec_cmd('rm ' + DATA_PATH + 'SpatialUnit.py')
    ssh.exec_cmd('rm ' + DATA_PATH + 'DBOperation.py')
    ssh.exec_cmd('rm ' + DATA_PATH + 'Unzip.py')
    ssh.close()
    print('Tools Scripts Removed :)')
