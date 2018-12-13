from SSHConnect import SSHConnect
import sys

if __name__ == '__main__':
    y_list = []
    # check for year params
    try:
        _ = sys.argv[1]
        y_list = sys.argv[1:]
    except IndexError as _:
        print('ERROR: No Year Param.')
        exit()
    ssh = SSHConnect(host='urbancolab.com',
                     username='root',
                     password='solokas')
    ssh.upload_file(local_file='/Users/zz/PycharmProjects/TaxiDataProcessing/scripts/SpatialUnit.py',
                    remote_path='/var/data/SpatialUnit.py')
    ssh.upload_file(local_file='/Users/zz/PycharmProjects/TaxiDataProcessing/scripts/DBOperation.py',
                    remote_path='/var/data/DBOperation.py')
    ssh.upload_file(local_file='/Users/zz/PycharmProjects/TaxiDataProcessing/scripts/Unzip.py',
                    remote_path='/var/data/Unzip.py')
    print('Tools Scripts Uploaded :)')
    for year in y_list:
        ssh.connect()
        ssh.upload_file(local_file='/Users/zz/PycharmProjects/TaxiDataProcessing/scripts/%s.py' % year,
                        remote_path='/var/data/%s.py' % year)
        ssh.exec_cmd('python /var/data/%s.py' % year)
        print('Script Executed :)')
        ssh.exec_cmd('rm /var/data/%s.py' % year)
        print('Script Removed :)')
        ssh.close()
    ssh.exec_cmd('rm /var/data/SpatialUnit.py')
    ssh.exec_cmd('rm /var/data/DBOperation.py')
    ssh.exec_cmd('rm /var/data/Unzip.py')
    print('Tools Scripts Removed :)')
