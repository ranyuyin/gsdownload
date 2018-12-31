from os import renames
from os.path import split, join
from glob import glob
from sys import argv
def parse_dst(filename,dstdirname):
    shortname = split(filename)[-1]
    Satellite,Sensor,path,row,date,product=shortname.split('-')
    dstname=join(dstdirname,path.lstrip('0'),row.lstrip('0'),shortname)
    return dstname
if __name__=='__main__':
    dirname=argv[1]
    dstdirname=argv[2]
    dirlist=glob(join(dirname,'*-*'))
    for thisdir in dirlist:
        dst=parse_dst(thisdir,dstdirname)
        print(thisdir+'\n')
        # print(dst+'\n')
        renames(thisdir,dst)