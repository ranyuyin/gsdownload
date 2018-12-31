from sys import argv
from os import path
from glob import glob
def add_sufix(filename, suffix, endname, opath):
    basename=path.basename(filename)
    suffix_filename = path.splitext(basename)[0]+endname+path.splitext(basename)[1]
    suffix_filename=path.join(opath,suffix_filename)
    suffix_file = open(suffix_filename, 'w')
    file=open(filename, 'r')
    rawlines = file.readlines()
    newline = [line.strip('\n')+suffix+'\n' for line in rawlines]
    suffix_file.writelines(newline)
    suffix_file.close()
if __name__ == '__main__':
    # suffix = argv[2]
    suffix = ['/L*B[6,5,4].TIF', '/L*BQA.TIF', '/L*MTL.txt']
    endnames = ['_654', '_BAQ', '_MTL']
    filelist=glob(path.join(argv[1],r'*.txt'))
    for file in filelist:
        for item, name in zip(suffix, endnames):
            add_sufix(file, item, name, argv[2])
