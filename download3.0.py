# coding:utf-8
# v3版本将所有景一起加入下载队列下载，区别于v1版本中逐景下载
# 已知问题：同时下载数量过多时，可能会报出连接错误

import os,sys

from glob import glob

if __name__=='__main__':
    todofolder=sys.argv[1]
    downloaddir=sys.argv[2]
    filelist=glob(os.path.join(todofolder,r'*.txt'))
    for file in filelist:
        f=open(file)
        urls=f.readlines()
        f.close()
        pr=os.path.splitext(os.path.basename(file))[0]
        dstdir=os.path.join(downloaddir,pr[:3],pr[3:])
        if not os.path.exists(dstdir):
            os.makedirs(dstdir)
        os.system('type {0} | gsutil -m cp -r -n -I {1}'.format(file, dstdir))
        os.remove(file)
