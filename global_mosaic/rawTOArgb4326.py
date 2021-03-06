import argparse
import traceback

import rasterio as rio
from functools import partial
from multiprocessing import Pool
from tqdm import tqdm
import os
from os import path, system
from glob import glob
import re
import numpy as np
import math
from datetime import datetime
from rasterio.warp import transform_bounds
import pandas as pd
import shutil


envs = {'temp': 'R:\\Temp'}


def _cast_to_best_type(kd):
    key, data = kd[0]
    try:
        return key, int(data)
    except ValueError:
        try:
            return key, float(data)
        except ValueError:
            return key, u'{}'.format(data.strip('"'))


def _parse_data(line):
    kd = re.findall(r'(.*)\s\=\s(.*)', line)

    if len(kd) == 0:
        return False, False
    else:
        return _cast_to_best_type(kd)


def _parse_mtl_txt(mtltxt):
    mtltxt = open(mtltxt).read()
    group = re.findall('.*\n', mtltxt)

    is_group = re.compile(r'GROUP\s\=\s.*')
    is_end = re.compile(r'END_GROUP\s\=\s.*')
    get_group = re.compile('\=\s([A-Z0-9\_]+)')

    output = [{
        'key': 'all',
        'data': {}
    }]

    for g in map(str.lstrip, group):
        if is_group.match(g):
            output.append({
                'key': get_group.findall(g)[0],
                'data': {}
            })

        elif is_end.match(g):
            endk = output.pop()
            k = u'{}'.format(endk['key'])
            output[-1]['data'][k] = endk['data']

        else:
            k, d = _parse_data(g)
            if k:
                k = u'{}'.format(k)
                output[-1]['data'][k] = d

    return output[0]['data']


def getOname(pid, outFolder, suffix=''):
    return path.join(outFolder, pid + '_4mosaic{}.pix'.format(suffix))


def toMosaic(mtlFile, outFolder, maskCloud, OVERWRITE, pixel_sunangle, keppTemp, maprgb, debug=False):
    try:
        landsatIm = LandsatDst(mtlFile, maprgb)
    except:
        print(mtlFile)
        return
    if debug:
        landsatIm.prepareMosaic(outFolder, maskCloud, OVERWRITE, pixel_sunangle, keppTemp)
    else:
        try:
            landsatIm.prepareMosaic(outFolder, maskCloud, OVERWRITE, pixel_sunangle, keppTemp)
        except:
            print(landsatIm.pid)
    return


def reflectance(img, MR, AR, E, src_nodata=0):
    """Calculate top of atmosphere reflectance of Landsat 8
    as outlined here: http://landsat.usgs.gov/Landsat8_Using_Product.php
    R_raw = MR * Q + AR
    R = R_raw / cos(Z) = R_raw / sin(E)
    Z = 90 - E (in degrees)
    where:
        R_raw = TOA planetary reflectance, without correction for solar angle.
        R = TOA reflectance with a correction for the sun angle.
        MR = Band-specific multiplicative rescaling factor from the metadata
            (REFLECTANCE_MULT_BAND_x, where x is the band number)
        AR = Band-specific additive rescaling factor from the metadata
            (REFLECTANCE_ADD_BAND_x, where x is the band number)
        Q = Quantized and calibrated standard product pixel values (DN)
        E = Local sun elevation angle. The scene center sun elevation angle
            in degrees is provided in the metadata (SUN_ELEVATION).
        Z = Local solar zenith angle (same angle as E, but measured from the
            zenith instead of from the horizon).
    Parameters
    -----------
    img: ndarray
        array of input pixels of shape (rows, cols) or (rows, cols, depth)
    MR: float or list of floats
        multiplicative rescaling factor from scene metadata
    AR: float or list of floats
        additive rescaling factor from scene metadata
    E: float or numpy array of floats
        local sun elevation angle in degrees
    Returns
    --------
    ndarray:
        float32 ndarray with shape == input shape
    """

    if np.any(E < 0.0):
        raise ValueError("Sun elevation must be nonnegative "
                         "(sun must be above horizon for entire scene)")

    input_shape = img.shape

    if len(input_shape) > 2:
        img = np.rollaxis(img, 0, len(input_shape))

    rf = ((MR * img.astype(np.float32)) + AR) / np.sin(np.deg2rad(E))
    if src_nodata is not None:
        rf[img == src_nodata] = 0.0

    if len(input_shape) > 2:
        if np.rollaxis(rf, len(input_shape) - 1, 0).shape != input_shape:
            raise ValueError(
                "Output shape %s is not equal to input shape %s"
                % (rf.shape, input_shape))
        else:
            return np.rollaxis(rf, len(input_shape) - 1, 0)
    else:
        return rf


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


def sun_elevation(bounds, shape, date_collected, time_collected_utc):
    """
    Given a raster's bounds + dimensions, calculate the
    sun elevation angle in degrees for each input pixel
    based on metadata from a Landsat MTL file
    Parameters
    -----------
    bounds: BoundingBox
        bounding box of the input raster
    shape: tuple
        tuple of (rows, cols) or (depth, rows, cols) for input raster
    collected_date_utc: str
        Format: YYYY-MM-DD
    collected_time: str
        Format: HH:MM:SS.SSSSSSSSZ
    Returns
    --------
    ndarray
        ndarray with shape = (rows, cols) with sun elevation
        in degrees calculated for each pixel
    """
    utc_time = parse_utc_string(date_collected, time_collected_utc)

    if len(shape) == 3:
        _, rows, cols = shape
    else:
        rows, cols = shape

    lng, lat = _create_lnglats((rows, cols),
                               list(bounds))

    decimal_hour = time_to_dec_hour(utc_time)

    declination = calculate_declination(utc_time.timetuple().tm_yday)

    return _calculate_sun_elevation(lng, lat, declination,
                                    utc_time.timetuple().tm_yday,
                                    decimal_hour)


def calculate_declination(d):
    """
    Calculate the declination of the sun in radians based on a given day.
    As reference +23.26 degrees at the northern summer solstice, -23.26
    degrees at the southern summer solstice.
    See: https://en.wikipedia.org/wiki/Position_of_the_Sun#Calculations
    Parameters
    -----------
    d: int
        days from midnight on January 1st
    Returns
    --------
    declination in radians: float
        the declination on day d
    """
    return np.arcsin(
        np.sin(np.deg2rad(23.45)) *
        np.sin(np.deg2rad(360. / 365.) *
               (d - 81))
    )


def solar_angle(day, utc_hour, longitude):
    """
    Given a day, utc decimal hour, and longitudes, compute the solar angle
    for these longitudes
    Parameters
    -----------
    day: int
        days of the year with jan 1 as day = 1
    utc_hour: float
        decimal hour of the day in utc time to compute solar angle for
    longitude: ndarray or float
        longitude of the point(s) to compute solar angle for
    Returns
    --------
    solar angle in degrees for these longitudes
    """
    localtime = (longitude / 180.0) * 12 + utc_hour

    lstm = 15 * (localtime - utc_hour)

    B = np.deg2rad((360. / 365.) * (day - 81))

    eot = (9.87 *
           np.sin(2 * B) -
           7.53 * np.cos(B) -
           1.5 * np.sin(B))

    return 15 * (localtime +
                 (4 * (longitude - lstm) + eot) / 60.0 - 12)


def _calculate_sun_elevation(longitude, latitude, declination, day, utc_hour):
    """
    Calculates the solar elevation angle
    https://en.wikipedia.org/wiki/Solar_zenith_angle
    Parameters
    -----------
    longitude: ndarray or float
        longitudes of the point(s) to compute solar angle for
    latitude: ndarray or float
        latitudes of the point(s) to compute solar angle for
    declination: float
        declination of the sun in radians
    day: int
        days of the year with jan 1 as day = 1
    utc_hour: float
        decimal hour from a datetime object
    Returns
    --------
    the solar elevation angle in degrees
    """
    hour_angle = np.deg2rad(solar_angle(day, utc_hour, longitude))

    latitude = np.deg2rad(latitude)

    return np.rad2deg(np.arcsin(
        np.sin(declination) *
        np.sin(latitude) +
        np.cos(declination) *
        np.cos(latitude) *
        np.cos(hour_angle)
    ))


def _create_lnglats(shape, bbox):
    """
    Creates a (lng, lat) array tuple with cells that respectively
    represent a longitude and a latitude at that location
    Parameters
    -----------
    shape: tuple
        the shape of the arrays to create
    bbox: tuple or list
        the bounds of the arrays to create in [w, s, e, n]
    Returns
    --------
    (lngs, lats): tuple of (rows, cols) shape ndarrays
    """

    rows, cols = shape
    xmin, s, xmax, n = bbox
    cross = False
    if xmin * xmax < 0 and xmax>170:
        e = 360 + xmin
        w = xmax
        cross = True
    else:
        w, e = xmin, xmax
    xCell = (e - w) / float(cols)
    yCell = (n - s) / float(rows)

    lat, lng = np.indices(shape, dtype=np.float32)
    if cross:
        lng, lat = (lng * xCell) + w + (xCell / 2.0), (np.flipud(lat) * yCell) + s + (yCell / 2.0)
        lng[lng>180] = lng[lng>180] - 360
        return lng, lat
    else:
        return ((lng * xCell) + w + (xCell / 2.0),
                (np.flipud(lat) * yCell) + s + (yCell / 2.0))


def parse_utc_string(collected_date, collected_time_utc):
    """
    Given a string in the format:
        YYYY-MM-DD HH:MM:SS.SSSSSSSSZ
    Parse and convert into a datetime object
    Fractional seconds are ignored
    Parameters
    -----------
    collected_date_utc: str
        Format: YYYY-MM-DD
    collected_time: str
        Format: HH:MM:SS.SSSSSSSSZ
    Returns
    --------
    datetime object
        parsed scene center time
    """
    utcstr = collected_date + ' ' + collected_time_utc

    if not re.match(r'\d{4}\-\d{2}\-\d{2}\ \d{2}\:\d{2}\:\d{2}\.\d+Z',
                    utcstr):
        raise ValueError("%s is an invalid utc time" % utcstr)

    return datetime.strptime(
        utcstr.split(".")[0],
        "%Y-%m-%d %H:%M:%S")


def time_to_dec_hour(parsedtime):
    """
    Calculate the decimal hour from a datetime object
    Parameters
    -----------
    parsedtime: datetime object
    Returns
    --------
    decimal hour: float
        time in decimal hours
    """
    return (parsedtime.hour +
            (parsedtime.minute / 60.0) +
            (parsedtime.second / 60.0 ** 2)
            )


def _main(args):
    inFolder = args.inFolder
    outFolder = args.outFolder
    if not path.exists(outFolder):
        os.mkdir(outFolder)
    # scaleFactor = args.scaleFactor
    n_multi = args.n_multi
    maskCloud = args.maskCloud
    OVERWRITE = args.OVERWRITE
    pixel_sunangle = args.pixel_sunangle
    keppTemp = args.keppTemp
    prList = args.path_row
    maprgb = args.rgb
    goodThumbDir = args.goodThumbDir
    # scan List
    cachMTLname = path.join(inFolder, '_mtlCach.csv')
    if args.cach and path.exists(cachMTLname):
        dfMTL = pd.read_csv(cachMTLname)
    else:
        mtlList = glob(path.join(inFolder, '**', '*MTL.txt'), recursive=True)
        dfMTL = pd.DataFrame(data={'mtl': mtlList})
        dfMTL['PR'] = [int(path.basename(mtl).split('_')[2]) for mtl in dfMTL.mtl]
        dfMTL['pid'] = [path.basename(mtl).split('_MTL')[0] for mtl in dfMTL.mtl]
        dfMTL.to_csv(cachMTLname, index=False)
    dfMTL = dfMTL.drop_duplicates(subset=['pid'])
    if path.exists(prList):
        print('using PR List.\n')
        dfTodoPr = pd.read_csv(prList)
        dfMTL = dfMTL.loc[dfMTL.PR.isin(dfTodoPr.PR)]
    if path.exists(goodThumbDir):
        print('using good thumb filter.\n')
        goodpid = glob(path.join(goodThumbDir, '*.jpg'))
        pids = [path.basename(i)[:-4] for i in goodpid]
        dfMTL = dfMTL.loc[dfMTL.pid.isin(pids)]
    if args.DEBUG:
        envs['temp'] = os.environ['temp']
        for mtl in tqdm(dfMTL.mtl):
            try:
                toMosaic(mtl,outFolder,maskCloud, OVERWRITE, pixel_sunangle, keppTemp, maprgb, args.DEBUG)
            except Exception as e:
                traceback.print_exc()
    else:
        p = Pool(n_multi)
        try:
            for i in tqdm(p.imap(partial(toMosaic, outFolder=outFolder, maskCloud=maskCloud, OVERWRITE=OVERWRITE,
                                         pixel_sunangle=pixel_sunangle, keppTemp=keppTemp, maprgb=maprgb), dfMTL.mtl), total=len(dfMTL)):
                pass
        except KeyboardInterrupt:
            p.terminate()
            p.join()

def getLeft(x1,x2):
    if (max(x1, x2) - min(x1, x2)) > 180:
        return max(x1, x2)
    else:
        return min(x1, x2)


def getRight(x1,x2):
    if (max(x1, x2) - min(x1, x2)) > 180:
        return min(x1, x2)
    else:
        return max(x1, x2)


class LandsatDst:
    rgbMap = {'LANDSAT_5': '543',
              'LANDSAT_7': '543',
              'LANDSAT_8': '654'}
    allMap = {'LANDSAT_8': '234567'}
    clearQaDict = {'LANDSAT_5': [672, 676, 680, 684],
                   'LANDSAT_7': [672, 676, 680, 684],
                   'LANDSAT_8': [2720, 2724, 2728, 2732]}

    def __init__(self, mtlfile, maprgb=False):
        workspace = path.split(mtlfile)[0]
        mtl = _parse_mtl_txt(mtlfile)
        metadata = mtl['L1_METADATA_FILE']
        self.maskcloud = False
        self.pixel_sunangle = False
        self.spacecraft = metadata['PRODUCT_METADATA']['SPACECRAFT_ID']
        if maprgb:
            self.bands = self.rgbMap[self.spacecraft]
        else:
            self.bands = self.allMap[self.spacecraft]
        self.clearQaValues = self.clearQaDict[self.spacecraft]
        self.date_collected = metadata['PRODUCT_METADATA']['DATE_ACQUIRED']
        self.time_collected_utc = metadata['PRODUCT_METADATA']['SCENE_CENTER_TIME']
        self.pid = metadata['METADATA_FILE_INFO']['LANDSAT_PRODUCT_ID']
        self.srcList = [path.join(workspace,
                                  metadata['PRODUCT_METADATA']['FILE_NAME_BAND_{}'.format(b)]
                                  ) for b in self.bands]
        self.qaBand = path.join(workspace,
                                metadata['PRODUCT_METADATA']['FILE_NAME_BAND_QUALITY']
                                )
        self.M = [metadata['RADIOMETRIC_RESCALING']
                  ['REFLECTANCE_MULT_BAND_{}'.format(b)]
                  for b in self.bands]
        self.A = [metadata['RADIOMETRIC_RESCALING']
                  ['REFLECTANCE_ADD_BAND_{}'.format(b)]
                  for b in self.bands]
        self.E = metadata['IMAGE_ATTRIBUTES']['SUN_ELEVATION']
        PRODUCT_META = metadata['PRODUCT_METADATA']
        LL_LON, LL_LAT = PRODUCT_META['CORNER_LL_LON_PRODUCT'], PRODUCT_META['CORNER_LL_LAT_PRODUCT']
        UL_LON, UL_LAT = PRODUCT_META['CORNER_UL_LON_PRODUCT'], PRODUCT_META['CORNER_UL_LAT_PRODUCT']
        UR_LON, UR_LAT = PRODUCT_META['CORNER_UR_LON_PRODUCT'], PRODUCT_META['CORNER_UR_LAT_PRODUCT']
        LR_LON, LR_LAT = PRODUCT_META['CORNER_LR_LON_PRODUCT'], PRODUCT_META['CORNER_LR_LAT_PRODUCT']

        self.xLeft = getLeft(LL_LON, UL_LON)
        self.xRight = getRight(UR_LON, LR_LON)
        self.yUp = max(UL_LAT, UR_LAT)
        self.yLow = min(LR_LAT, LL_LAT)
        self.count = len(self.bands)
        self.tempOutName = path.join(envs['temp'], self.pid + '_out.tif')
        if (max(self.xLeft, self.xRight) - min(self.xLeft, self.xRight)) > 180:
            self.cross180 = True
        else:
            self.cross180 = False
        self.oRes = 0.00025


    def toRTM(self):
        # todo mask cloud
        srcList = [rio.open(i) for i in self.srcList]
        with rio.open(self.qaBand) as qaSrc:
            # srcList = [redset, greenset, blueset]
            meta = srcList[0].meta.copy()
            self.crs = meta['crs']
            meta.update({'COMPRESS': 'LZW',
                         'dtype': 'uint8',
                         'count': self.count,
                         'nodata': 0})
            with rio.open(self.tempOutName, 'w', **meta) as dst:
                for ji, window in srcList[0].block_windows(1):
                    imBands = [src.read(1, window=window) for src in srcList]
                    im = np.stack(imBands)
                    imqa = qaSrc.read(1, window=window)
                    if self.maskcloud:
                        clearMask = np.zeros(imBands[0].shape, dtype=np.bool)
                        for clearValue in self.clearQaValues:
                            clearMask[imqa == clearValue] = True
                    _, rows, cols = im.shape
                    if self.pixel_sunangle:
                        bbox = rio.coords.BoundingBox(
                            *transform_bounds(
                                self.crs,
                                {'init': u'epsg:4326'},
                                *srcList[0].window_bounds(window), 0))
                        E = sun_elevation(
                            bbox,
                            (rows, cols),
                            self.date_collected,
                            self.time_collected_utc).reshape(rows, cols, 1)
                    else:
                        E = np.array([self.E for i in range(self.count)])
                    if self.maskcloud:
                        mask = clearMask == False
                    else:
                        mask = imqa == 1
                        mask[imqa==0] = True
                    rgb = reflectance(im, self.M, self.A, E).clip(min=0, max=0.55) * (254 / 0.55) + 1
                    rgb[:, mask] = 0
                    dst.write(rgb.astype('uint8'), list(range(1,self.count+1)), window=window)
        if self.maskcloud:
            qaSrc = None


    def prepareMosaic(self, mosaicOfolder, maskCloud=False, OVERWRITE=False, pixel_sunangle=False, keppTemp=False):
        if not OVERWRITE:
            if path.exists(getOname(self.pid, mosaicOfolder)):
                return
            elif path.exists(getOname(self.pid, mosaicOfolder, '1')) and path.exists(getOname(self.pid, mosaicOfolder, '2')):
                return

        self.maskcloud = maskCloud
        self.pixel_sunangle = pixel_sunangle
        self.toRTM()
        # -dstnodata 0
        cmdBase = 'gdalwarp -t_srs EPSG:4326 -co COMPRESSION=RLE -overwrite -q' \
                  ' -of PCIDSK -co TILESIZE=256 -co INTERLEAVING=TILED -r cubic -wm 3000 -srcnodata 0'
        cmdVar = ' -te {} {} {} {} -te_srs EPSG:4326 -tr {} {} {} {}'
        if self.cross180:
            te = tapRes(self.xLeft, self.yLow, self.xRight, self.yUp, self.oRes, self.cross180)
            oP1 = getOname(self.pid, mosaicOfolder, '1')
            oP2 = getOname(self.pid, mosaicOfolder, '2')
            oP1t = getOname(self.pid, envs['temp'], '1')
            oP2t = getOname(self.pid, envs['temp'], '2')
            cmd1 = cmdBase + cmdVar.format(*te[0], self.oRes, self.oRes, self.tempOutName, oP1t)
            cmd2 = cmdBase + cmdVar.format(*te[1], self.oRes, self.oRes, self.tempOutName, oP2t)
            # print(cmd1)
            # print(cmd2)
            system(cmd1)
            system(cmd2)
            shutil.move(oP1t, oP1)
            shutil.move(oP2t, oP2)
        else:
            te = tapRes(self.xLeft, self.yLow, self.xRight, self.yUp, self.oRes, self.cross180)
            oP = getOname(self.pid, mosaicOfolder)
            oPt = getOname(self.pid, envs['temp'])
            cmd = cmdBase + cmdVar.format(*te, self.oRes, self.oRes, self.tempOutName, oPt)
            # print(cmd)
            system(cmd)
            shutil.move(oPt, oP)
        if keppTemp:
            return
        else:
            try:
                os.remove(self.tempOutName)
            except:
                print('Delete {} error'.format(path.basename(self.tempOutName)))
            return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Produce Landsat TOA product.')
    parser.add_argument('-i', '--in', metavar='indir', dest='inFolder', required=True,
                        help='input directory')
    parser.add_argument('-o', '--out', metavar='outdir', dest='outFolder', required=True,
                        help='output directory')
    parser.add_argument('--pr', metavar='path row filter', dest='path_row', required=False,
                        default='', help='path row list ')
    parser.add_argument('--gooddir', metavar='good dir pid filter', dest='goodThumbDir', required=False,
                        default='', help='good dir pid filter ')
    parser.add_argument('-m', metavar='multi_process', dest='n_multi', required=False, type=int,
                        default=20, help='band(s) to mask')
    parser.add_argument('--overwrite', action='store_true', dest='OVERWRITE')
    parser.add_argument('--oneangle', action='store_false', dest='pixel_sunangle')
    parser.add_argument('--keeptemp', action='store_true', dest='keppTemp')
    parser.add_argument('--maskcloud', action='store_true', dest='maskCloud')
    parser.add_argument('--DEBUG', action='store_true', dest='DEBUG')
    parser.add_argument('--nocach', action='store_false', dest='cach')
    parser.add_argument('--rgb', action='store_true', dest='rgb')
    args = parser.parse_args()
    # print(args.outFolder[0])
    # print(args)
    _main(args)

    # print(args.accumulate(args.integers))

    exit(0)
