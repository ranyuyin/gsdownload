from os import walk,path,renames
from sys import argv
from glob import glob
import re
def moveone(fullpath):
    father,thisbase = path.split(fullpath)
    partten = '(.*)_.*\..*'
    folder = re.split(partten, thisbase)[1]
    print(fullpath)
    renames(fullpath, path.join(father, folder, thisbase))
if __name__ == '__main__':
    root_dir = argv[1]
    alllist = glob(path.join(root_dir, '**','*.*'), recursive = True)
    for onefile in alllist:
        moveone(onefile)