from os import path
from shutil import copy, copytree
from glob import glob
import re
from tqdm import tqdm

if __name__ == '__main__':
    thumbsDir = r'Z:\yinry\Inbox\cngs\0.thumbnail\selected'
    srcRoot = r'Z:\yinry\Landsat.Data\google_l7_yinranyu'
    dstRoot = r'Z:\yinry\Inbox\cngs\datacollect'
    elogfile = thumbsDir+'.elog.txt'
    thumbList = glob(path.join(thumbsDir,'*.jpg'))
    pidList = [path.splitext(path.basename(i))[0] for i in thumbList]
    parten = '^.{4}_.{4}_(...)(...)_(.{8}).*'
    ef = open(elogfile, 'w')
    for onePID in tqdm(pidList):
        _, wrs_path, wrs_row, date, _ = re.split(parten, onePID)
        srccondi = glob(path.join(srcRoot, wrs_path, wrs_row, '*_{}{}_{}*'.format(wrs_path, wrs_row, date)))
        if len(srccondi) is not 1:
            ef.write('{}\n'.format(onePID))
            continue
        else:
            continue
            dstthis = path.join(dstRoot,onePID)
            copytree(srccondi[0], dstthis)
    ef.close()