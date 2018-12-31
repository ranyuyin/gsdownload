from os import path,renames
from glob import glob
from sys import argv
import time
def parse_dst(filename):
    dstname = path.join(path.split(filename)[0], '_'.join(path.split(filename)[1].split('_')[:-1]),
                        path.split(filename)[1])
    return dstname
if __name__=='__main__':
    dirname=argv[1]
    while(True):
        filelist=glob(path.join(dirname,'*.TIF'))
        for file in filelist:
            dst=parse_dst(file)
            # print(file+'\n')
            # print(dst+'\n')
            renames(file,dst)
        print('moved {0} file'.format(len(filelist)))
        time.sleep(180)