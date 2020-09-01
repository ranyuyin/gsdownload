import down_util
import argparse
import pandas as pd
from os import path, makedirs
from functools import partial
from datetime import datetime
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool

def LandsatMetaRead(meta_csv):
    dfMeta = pd.read_csv(meta_csv, usecols=['path', 'row', 'sensor', 'LANDSAT_PRODUCT_ID',
                                            'browseURL', 'browseAvailable', 'acquisitionDate',
                                            'sceneID', 'COLLECTION_CATEGORY', 'DATA_TYPE_L1'],
                         parse_dates=['acquisitionDate'])
    return dfMeta


def GetMetaDf(candiFL, SLCoff=False):
    metaAll = pd.DataFrame()
    for csvP in candiFL:
        oneF = LandsatMetaRead(csvP)
        if path.basename(csvP)=='LANDSAT_ETM_C1.csv.gz':
            if SLCoff is not True:
                oneF = oneF.loc[oneF.acquisitionDate < datetime(2003, 5, 31)]
        metaAll = metaAll.append(oneF, ignore_index=True)
    return metaAll

# def GetDatasetList(start_date,end_date):
#     # candiL = ['MSS', 'TM', 'ETM', '8']
#     candiL = ['TM', 'ETM', '8']
#     if start_date > datetime(year=2013):
#         candiL.remove('ETM')
#         candiL.remove('MSS')
#         candiL.remove('TM')
#         # to do
#

if __name__ == '__main__':
    metaDir = r'Z:\yinry\0.DEFINITION\LandsatMetaDataUSGS'
    # candiL = ['MSS', 'TM', 'ETM', '8']
    candiL = ['TM', 'ETM', '8']
    candiFL = [path.join(metaDir, 'LANDSAT_{}_C1.csv.gz'.format(i)) for i in candiL]
    dfMeta = GetMetaDf(candiFL)
    parser = argparse.ArgumentParser(description='Get thumbnails by pr')
    parser.add_argument('-s', help='start date', dest='start_date', required=True)
    parser.add_argument('-e', help='end date', dest='end_date', required=True)
    parser.add_argument('-w', help='work dir', dest='work_dir', required=True)
    parser.add_argument('-d', help='path row definition', dest='prs',
                        required=False, default=r"Z:\yinry\global_mosaic\0.def\prwithrange0621.csv")
    parser.add_argument('-i', help='index file', dest='gstable',
                        required=False, default=r'Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz')
    parser.add_argument('-m', help='multi process', dest='nMulti', required=False, default=15, type=int)
    parser.add_argument('--badin', action='store_false', dest='goodOnly')
    args = parser.parse_args()
    datepaser = '%Y-%m-%d'
    date_start, date_end = datetime.strptime(args.start_date, datepaser), datetime.strptime(args.end_date, datepaser)
    dfG, _ = down_util.split_collection(args.gstable)
    dfG = dfG.loc[(dfG.SPACECRAFT_ID != 'LANDSAT_7') | (dfG.DATE_ACQUIRED < datetime(year=2003, month=5, day=31))]
    subDfG = dfG.loc[(dfG.DATE_ACQUIRED >= date_start) &
                     (dfG.DATE_ACQUIRED <= date_end), ['SCENE_ID', 'BASE_URL']]
    prlist = pd.read_csv(args.prs, dtype={'PR': str})
    thumb_root = path.join(args.work_dir, '0.thumbnail')
    if path.exists(thumb_root) is False:
        makedirs(thumb_root)
    subDf = dfMeta.loc[(dfMeta.acquisitionDate >= date_start) &
                       (dfMeta.acquisitionDate <= date_end)]
    prDf = [str(i[0]) + str(i[1]).zfill(3) for i in zip(subDf.path, subDf.row)]
    subDf['PR'] = prDf
    subDf = subDf.loc[subDf.PR.isin(prlist.PR)]
    subDf = subDf.sort_values(by='PR')
    subDf = subDf.loc[~(subDf.COLLECTION_CATEGORY == 'RT')]
    subDf = subDf.loc[~subDf.DATA_TYPE_L1.str.endswith('L1GS')]
    if args.goodOnly:
        print('using only L1TP if available')
        subDfL1T = subDf.loc[subDf.DATA_TYPE_L1.str.endswith('L1TP')]
        grouped = subDfL1T.groupby('PR')
        gpcounts = grouped['DATA_TYPE_L1'].agg(np.size)
        # lowlevel = 0.1*gpcounts.max()
        # lowlevel = 2 if lowlevel < 2 else lowlevel
        DeL1GTPr = gpcounts.loc[gpcounts >= 2].index
        subDf = subDf.loc[~(subDf.PR.isin(DeL1GTPr)&(~subDf.DATA_TYPE_L1.str.endswith('L1TP')))]
    subDf = subDf.merge(subDfG, 'left', left_on='sceneID', right_on='SCENE_ID')
    subDf = subDf.drop_duplicates('LANDSAT_PRODUCT_ID')
    subDf.to_csv(path.join(thumb_root, 'candiDf.csv'), index=False)
    f = open(path.join(thumb_root, 'urlThumbD.csv'), 'w')
    for url in subDf.browseURL:
        wrsPath = url.split('/')[6]
        wrsrow = url.split('/')[7]
        opath = path.join(thumb_root, wrsPath, wrsrow)
        f.write(url+'\n')
        f.write('\t'+'dir='+ opath+'\n')
