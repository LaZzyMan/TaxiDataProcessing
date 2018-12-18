import gzip
import os
import zipfile
import tarfile


def un_zip(file_name):
    folder_name = file_name.replace('.zip', '')
    zip_file = zipfile.ZipFile(file_name)
    if os.path.isdir(folder_name):
        pass
    else:
        os.mkdir(folder_name)
    for names in zip_file.namelist():
        zip_file.extract(names, folder_name + '/')
    zip_file.close()
    return folder_name


def un_gz(file_name):
    f_name = file_name.replace('.gz', '')
    g_file = gzip.GzipFile(file_name)
    open(f_name, 'wb+').write(g_file.read())
    g_file.close()


def un_tar(file_name):
    folder_name = file_name.replace('.tar', '')
    tar = tarfile.open(file_name)
    names = tar.getnames()
    if os.path.isdir(folder_name):
        pass
    else:
        os.mkdir(folder_name)
    for name in names:
        tar.extract(name, folder_name + '/')
    tar.close()
    return folder_name


def un_tar_gz(file_name):
    un_gz(file_name)
    return un_tar(file_name.replace('.gz', ''))


def decompress(file_name):
    os.system('java -jar Decompress.jar ' + file_name)
    return file_name.replace('.tar.gz', '')


if __name__ == '__main__':
    un_tar_gz('gnuplot.tar.gz')
