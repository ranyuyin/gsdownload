import argparse
import pandas as pd
import os, re, shutil
from glob import glob
from os import path
from skimage import io
import numpy as np
from datetime import datetime
import wget
from joblib import Parallel, delayed
from tqdm import tqdm

def add_prefix(filename, prefix):
    return path.join(path.split(filename)[0], prefix+'_'+path.split(filename)[1])

def split_collection(filename, write=False, nrows=None):
    df = pd.read_csv(filename, usecols=['SCENE_ID', 'PRODUCT_ID', 'SENSOR_ID', 'SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                             'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL','NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'], nrows=nrows)
    c1 = df.loc[df.COLLECTION_NUMBER=='01']
    cpre = df.loc[df.COLLECTION_NUMBER=='PRE']
    if write:
        c1.to_csv(add_prefix(filename,'c1'), index=False, compression='gzip')
        cpre.to_csv(add_prefix(filename,'cpre'), index=False, compression='gzip')
    return c1, cpre

def filtermonth(df, monthlist):
    df = addyearmonth(df)
    df = df.loc[df['month'].isin(monthlist)]
    return df

def addyearmonth(df):
    df.reset_index(inplace=True, drop=True)
    # year_month_list = [(item.year, item.month) for item in df.DATE_ACQUIRED]
    month_list = [item.month for item in df.DATE_ACQUIRED]
    year_list = [item.year for item in df.DATE_ACQUIRED]
    df['year'] = year_list
    df['month'] = month_list
    # year_month_pd = pd.DataFrame(year_month_list, columns=['year', 'month'])
    return df

def pr_from_pid(pid):
    parten = '^.{4}_.{4}_(...)(...)_(.{4})(..)(..).*'
    _, wrs_path, wrs_row, _, _, _, _ = re.split(parten, pid)
    return str(int(wrs_path))+wrs_row

def hist_score(imgQ, imgD, bins=51, inrange=(0, 255)):
    from skimage import color
    scores = []
    for i in range(3):
        histQ, _ = np.histogram(imgQ[:, :, i].flatten(), bins=bins, range=inrange)
        histQ = histQ/(imgQ.shape[0] * imgQ.shape[1])
        histD, _ = np.histogram(imgD[:, :, i].flatten(), bins=bins, range=inrange)
        histD = histD/(imgD.shape[0] * imgD.shape[1])
        scores.append(np.vstack((histQ, histD)).min(axis=0).sum() / histQ.sum())
    hsvQ = color.rgb2hsv(imgQ)
    hsvD = color.rgb2hsv(imgD)
    for i in range(3):
        histQ, _ = np.histogram(hsvQ[:, :, i].flatten(), bins=bins, range=(0, 1))
        histQ = histQ/(hsvQ.shape[0] * hsvQ.shape[1])
        histD, _ = np.histogram(hsvD[:, :, i].flatten(), bins=bins, range=(0, 1))
        histD = histD/(hsvD.shape[0] * hsvD.shape[1])
        scores.append(np.vstack((histQ, histD)).min(axis=0).sum() / histQ.sum())
    scores = np.array(scores)
    return scores.mean()

def Get_candi_by_onepr(wrs_path, wrs_row, date_start, date_end, df, ignoreSLCoff=True, monthlist=None, n_condi=25):
    pd_condi = pd.DataFrame()
    if monthlist is not None:
        df = filtermonth(df, monthlist)
    if ignoreSLCoff:
        df = df.loc[(df.SPACECRAFT_ID != 'LANDSAT_7') | (df.DATE_ACQUIRED < datetime(year=2003, month=5, day=31))]
    thiscandi = df.loc[(df.WRS_PATH == wrs_path) &
                       (df.WRS_ROW == wrs_row) &
                       (df.DATE_ACQUIRED > date_start) &
                       (df.DATE_ACQUIRED < date_end)]
    thiscandi = thiscandi.sort_values('CLOUD_COVER').head(n_condi)
    return thiscandi

def get_thumbnail_pid(pid, year, wrs_path, wrs_row, craft_id, download_root, errorlist, downlist=None):
    dirmaps = {'LANDSAT_1': 'landsat_mss_c1',
               'LANDSAT_2': 'landsat_mss_c1',
               'LANDSAT_3': 'landsat_mss_c1',
               'LANDSAT_4': 'landsat_mss_c1',
               'LANDSAT_5': 'landsat_tm_c1',
               'LANDSAT_7': 'landsat_etm_c1',
               'LANDSAT_8': 'landsat_8_c1'}
    baseurl = 'https://ims.cr.usgs.gov/browse/'
    if year is not None:
        thumbnail_url = baseurl + '/'.join([dirmaps[craft_id], year, wrs_path, wrs_row, pid + '.jpg'])
    else:
        year = pid.split('_')[3][:4]
        thumbnail_url = baseurl + '/'.join([dirmaps[craft_id], year, wrs_path, wrs_row, pid + '.jpg'])
    download_folder = path.join(download_root, wrs_path, wrs_row)
    while not path.exists(download_folder):
        try:
            os.makedirs(download_folder)
        except:
            pass
    try:
        down_file = path.join(download_folder, pid+'.jpg')
        # print(downlist)
        if downlist is not None:
            downlist.append(down_file)
        # down_file = path.join(
        #     download_folder, pid+'.jpg') if CLOUD_COVER is None else path.join(
        #     download_folder, "{0:.2f}".format(CLOUD_COVER)+'_'+pid+'.jpg')
        if not path.exists(down_file):
            # print(thumbnail_url, down_file)
            # print('download {}'.format(thumbnail_url))
            wget.download(thumbnail_url, down_file)
        else:
            pass
            # print('skip!')
    except:
        errorlist.append(pid)

def download_c1df_thumbnail(df, download_root, par=False):
    # df = addyearmonth(df)
    i = 0
    errorlist = []
    downlist = []
    def download_row(row, errorlist, downlist=None):
        try:
            year = str(row.year)
        except:
            year = None
        wrs_path = str(row.WRS_PATH).zfill(3)
        wrs_row = str(row.WRS_ROW).zfill(3)
        pid = row.PRODUCT_ID
        craft_id = row.SPACECRAFT_ID
        # CLOUD_COVER = row.CLOUD_COVER
        # print('downloading: ', pid)
        get_thumbnail_pid(pid, year, wrs_path, wrs_row, craft_id, download_root, errorlist, downlist)
        # print('done!')
    if par:
        Parallel(n_jobs=2, require='sharedmem')(delayed(download_row)(row, errorlist, downlist) for row in tqdm(df.itertuples()))
    else:
        for row in tqdm(df.itertuples()):
            download_row(row, errorlist, downlist)
    pd.DataFrame(data={'error_pid': errorlist}).to_csv(path.join(download_root, 'errorlist.csv'), index=False)
    if downlist is not None:
        return downlist

def Getprbest(ref_path, date_start=None, date_end=None, df=None, thumb_root=None, ignoreSLCoff=True, debug=False,
              datepaser='%Y-%m-%d', copydir='', monthlist=None):
    date_start, date_end = datetime.strptime(date_start, datepaser), datetime.strptime(date_end, datepaser)
    # Get candidate PID list for df
    pr = path.splitext(path.basename(ref_path))[0]
    print('processing {}'.format(pr))
    wrs_path, wrs_row = int(pr[:3]), int(pr[3:])
    cach_candi = '{}_{}_{}_{}.csv'.format(str(wrs_path).zfill(3),
                                                                 str(wrs_row).zfill(3),
                                                                 datetime.strftime(date_start, '%Y%m%d'),
                                                                 datetime.strftime(date_end, '%Y%m%d'))
    cach_candi_path = path.join(thumb_root, cach_candi)
    if not path.exists(cach_candi_path):
        candi_df = Get_candi_by_onepr(wrs_path, wrs_row, date_start, date_end, df,
                                      ignoreSLCoff=ignoreSLCoff, monthlist=monthlist)
        candi_df.to_csv(cach_candi_path)
    else:
        candi_df = pd.read_csv(cach_candi_path)
    candi_jpg_list = download_c1df_thumbnail(candi_df, thumb_root)
    print('{} has {} candi'.format(pr, len(candi_jpg_list)))
    if len(candi_jpg_list) == 0:
        return None
    imgQ = io.imread(ref_path)
    scores = []
    CCS = []
    # print(candi_jpg_list)
    for id, candi_img in enumerate(candi_jpg_list):
        if not path.exists(candi_img):
            scores.append(0)
            CCS.append(100)
            continue
        imgD = io.imread(candi_img)
        this_score = hist_score(imgQ, imgD)
        if debug:
            newname = add_prefix(candi_img, "{0:.2f}".format(this_score))
            os.renames(candi_img, newname)
            candi_jpg_list[id] = newname
        scores.append(this_score)
        pid = path.splitext(path.basename(candi_img))[0]
        CCS.append(list(candi_df.loc[candi_df.PRODUCT_ID == pid].CLOUD_COVER)[0])
    scores = np.array(scores)
    CCS = np.array(CCS)
    # CCS[scores < 0.5] = 100
    # CCS[scores < 0.2] = 100
    if CCS.min() == 100:
        print('{} failed!'.format(pr))
        return None
    best = candi_jpg_list[scores.argmin()]
    if copydir is not '':
        if best is not None:
            shutil.copy(best, copydir)
    return best

def one_best_worker(args, date_start, date_end, df, thumb_root, copydir='',
                    ignoreSLCoff=True, debug=False, datepaser='%Y-%m-%d'):
    ref_path, m_start, m_end = args
    # print(ref_path)
    if m_start is None or m_end is None:
        m_start, m_end = 1, 12
    if not path.exists(ref_path):
        return None
    else:
        if m_end < m_start:
            m_end += 12
        monthlist = [i % 12 if (i % 12) != 0 else 12 for i in list(range(m_start, m_end+1))]
        best = Getprbest(ref_path, date_start, date_end, df, thumb_root, ignoreSLCoff=ignoreSLCoff,
                  debug=debug, datepaser=datepaser, monthlist=monthlist)
        if copydir is not '':
            if best is not None:
                shutil.copy(best, copydir)
        return best

def BestsceneWoker(ref_root, prlistfile, date_start, date_end, thumb_root,
                   ignoreSLCoff=True, debug=False, datepaser='%Y-%m-%d', copydir='',
                   df=None, Global_monthlist=None, nprocess=4, PRnames=None):
    # from pathos.multiprocessing import ProcessPool
    from functools import partial
    from multiprocessing import Pool
    if copydir is not '' and path.exists(copydir) is False:
        os.makedirs(copydir)
    if path.exists(thumb_root) is False:
        os.makedirs(thumb_root)
    if df is None:
        df, _ = split_collection(r"Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz")
    if Global_monthlist is not None and type(Global_monthlist) is list:
        df = filtermonth(df, Global_monthlist)
    bestlist = []
    prlist = pd.read_csv(prlistfile, names=PRnames, dtype={'PR': str})
    if copydir is not '':
        finish_list = glob(path.join(copydir, '*.jpg'))
        jpglist = [path.basename(i) for i in finish_list]
        finishpr = [pr_from_pid(i) for i in jpglist]
        # print(len(prlist))
        prlist = prlist.loc[~prlist.PR.isin(finishpr)]
        # print(len(prlist))
    if 'start_mon' in prlist.columns and 'end_mon' in prlist.columns:
        m_start_list = list(prlist.start_mon)
        m_end_list = list(prlist.end_mon)
    else:
        m_start_list = [None] * len(prlist)
        m_end_list = [None] * len(prlist)
    ref_path_list = [path.join(ref_root, pr.zfill(6) + '.tif') for pr in prlist.PR]
    print('list done!')
    print('start!')
    p = Pool(nprocess)
    bestlist = p.map(partial(one_best_worker, date_start=date_start, date_end=date_end, df=df, thumb_root=thumb_root, copydir=copydir,
                    ignoreSLCoff=ignoreSLCoff, debug=debug, datepaser=datepaser), list(zip(ref_path_list, m_start_list, m_end_list)))
    return bestlist

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get best scene automatically')
    parser.add_argument('-s', help='start date', dest='start_date', required=True)
    parser.add_argument('-e', help='end date', dest='end_date', required=True)
    parser.add_argument('-w', help='work dir', dest='work_dir', required=True)
    parser.add_argument('-d', help='path row definition', dest='prs',
                        required=False, default=r"Z:\yinry\global_mosaic\0.def\prwithrange.csv")

    parser.add_argument('-r', help='reference root', dest='ref_root',
                        required=False, default=r'Z:\yinry\global_mosaic\global_thumbnail_ref\global_thumbnail_0203')
    parser.add_argument('-i', help='index file', dest='gstable',
                        required=False, default=r'Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz')
    parser.add_argument('-m', help='multi process', dest='nMulti', required=False, default=15, type=int)
    args = parser.parse_args()
    df, _ = split_collection(args.gstable)
    thumb_root = path.join(args.work_dir, '0.thumbnail')
    good = BestsceneWoker(args.ref_root, args.prs, args.start_date, args.end_date, thumb_root,
                                    copydir=path.join(args.work_dir, '1.good'), df=df, Global_monthlist=[4,5,6,7,8,9,10],
                                    nprocess=args.nMulti)
    pd.DataFrame(data={'good': good}).to_csv(path.join(thumb_root, 'good.csv'), index=False, header=False)
    exit(0)
