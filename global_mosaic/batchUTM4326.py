import math
import os
import shutil
from functools import partial
from glob import glob
from multiprocessing import Pool
from sys import argv
from rasterio.warp import transform_bounds
import rasterio as rio
from tqdm import tqdm

envs = {'temp': 'R:\\Temp'}


class utmim:
    cross180 = False
    oRes = 0.0025
    def __init__(self, imPath):
        with rio.open(imPath) as src:
            self.geoBounds = rio.coords.BoundingBox(
                *transform_bounds(
                    src.crs,
                    {'init': u'epsg:4326'},
                    *src.bounds))
            self.imPath = imPath
            self.pid = os.path.splitext(os.path.basename(imPath))[0]
            if self.geoBounds.left * self.geoBounds.right < 0 and self.geoBounds.right > 170:
                self.cross180 = True

    def runTrans(self, mosaicOfolder):
        cmdBase = 'gdalwarp -t_srs EPSG:4326 -co COMPRESSION=RLE -overwrite -q' \
                  ' -of PCIDSK -co TILESIZE=256 -co INTERLEAVING=TILED -r cubic -wm 3000 -srcnodata 0'
        cmdVar = ' -te {} {} {} {} -te_srs EPSG:4326 -tr {} {} {} {}'
        if self.cross180:
            te = tapRes(self.geoBounds.left, self.geoBounds.bottom,
                        self.geoBounds.right, self.geoBounds.top, self.oRes, self.cross180)
            oP1 = getOname(self.pid, mosaicOfolder, '1')
            oP2 = getOname(self.pid, mosaicOfolder, '2')
            oP1t = getOname(self.pid, envs['temp'], '1')
            oP2t = getOname(self.pid, envs['temp'], '2')
            cmd1 = cmdBase + cmdVar.format(*te[0], self.oRes, self.oRes, self.imPath, oP1t)
            cmd2 = cmdBase + cmdVar.format(*te[1], self.oRes, self.oRes, self.imPath, oP2t)
            # print(cmd1)
            # print(cmd2)
            os.system(cmd1)
            os.system(cmd2)
            shutil.move(oP1t, oP1)
            shutil.move(oP2t, oP2)
        else:
            te = tapRes(self.geoBounds.left, self.geoBounds.bottom,
                        self.geoBounds.right, self.geoBounds.top, self.oRes, self.cross180)
            oP = getOname(self.pid, mosaicOfolder)
            oPt = getOname(self.pid, envs['temp'])
            cmd = cmdBase + cmdVar.format(*te, self.oRes, self.oRes, self.imPath, oPt)
            # print(cmd)
            os.system(cmd)
            shutil.move(oPt, oP)


def tapRes(left, lowwer, right, upper, res, cross180):
    # xmin, ymin, xmax, ymax
    if not cross180:
        left = math.floor(left / res) * res
        lowwer = math.floor(lowwer / res) * res
        right = math.ceil(right / res) * res
        upper = math.ceil(upper / res) * res
        return left, lowwer, right, upper
    else:
        xmin1 = -180
        ymin = math.floor(lowwer / res) * res
        xmax1 = math.ceil(right / res) * res
        ymax = math.ceil(upper / res) * res

        xmin2 = math.floor(left / res) * res
        xmax2 = 180
        return (xmin1, ymin, xmax1, ymax), (xmin2, ymin, xmax2, ymax)


def getOname(pid, outFolder, suffix=''):
    return os.path.join(outFolder, pid + '_4mosaic{}.pix'.format(suffix))


def transOne(imFile, ofolder):
    im = utmim(imFile)
    im.runTrans(ofolder)
if __name__ == '__main__':
    rootdir = argv[1]
    outFolder = argv[2]
    imlist = glob(os.path.join(rootdir, '*.tif'))
    p = Pool(30)
    try:
        for i in tqdm(p.imap(partial(transOne, ofolder=outFolder), imlist), total=len(imlist)):
            pass
    except KeyboardInterrupt:
        p.terminate()
        p.join()