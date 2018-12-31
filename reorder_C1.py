from os import renames
from os.path import split, join
from glob import glob
from sys import argv
def parse_dst(filename,dstdirname):
    shortname = split(filename)[-1]
    nameparts=shortname.split('_')
    path=nameparts[2][:3]
    row=nameparts[2][3:]
    dstname=join(dstdirname,path,row,shortname)
    return dstname
if __name__=='__main__':
    dirname=argv[1]
    dstdirname=argv[2]
    dirlist=glob(join(dirname,'*_*'))
    for thisdir in dirlist:
        dst=parse_dst(thisdir,dstdirname)
        print(thisdir+'\n')
        # print(dst+'\n')
        renames(thisdir,dst)