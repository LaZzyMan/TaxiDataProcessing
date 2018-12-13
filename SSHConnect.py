import paramiko


class SSHConnect:
    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password
        self.ssh_fd = paramiko.SSHClient()

    def connect(self):
        try:
            self.ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_fd.connect(hostname=self.host,
                                username=self.username,
                                password=self.password)
        except Exception as e:
            print('ssh %s@%s: %s' % (self.username, self.host, e))
            exit()

    def exec_cmd(self, cmd):
        _, stdout, stderr = self.ssh_fd.exec_command(cmd)
        for err in stderr.readlines():
            print('ERROR: ' + err)
        for out in stdout.readlines():
            print(out)

    def upload_file(self, local_file, remote_path):
        t = paramiko.Transport((self.host, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        sftp.put(local_file, remote_path)
        t.close()
        print("Successfully upload file: %s :)" % local_file)

    def close(self):
        self.ssh_fd.close()


if __name__ == '__main__':
    # test class functions
    ssh = SSHConnect(host='urbancolab.com',
                     username='ucl',
                     password='solokas')
    ssh.connect()
    ssh.exec_cmd('ls')
    ssh.close()
    ssh.upload_file(local_file='/Users/zz/PycharmProjects/TaxiDataProcessing/SSHConnect.py',
                    remote_path='/home/ucl/DNT/SSHConnect.py')
