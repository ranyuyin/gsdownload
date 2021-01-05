import down_util
import argparse
import pandas as pd
from os import path, makedirs
from functools import partial
from datetime import datetime
import numpy as np
from tqdm import tqdm
from multiprocessing import Pool
from glob import glob

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
if __name__ == '__main__':
    metaDir = r'Z:\yinry\0.DEFINITION\LandsatMetaDataUSGS'
    QIroot = r'C:\Users\ranyu\Downloads\hsvDistance2000'
    thumb_root = r'C:\Users\ranyu\Downloads\hsvDisThumb'
    # candiL = ['MSS', 'TM', 'ETM', '8']
    candiL = ['TM', 'ETM']
    candiFL = [path.join(metaDir, 'LANDSAT_{}_C1.csv.gz'.format(i)) for i in candiL]
    dfMeta = GetMetaDf(candiFL)
    QIflist = glob(path.join(QIroot,'*.csv'))
    QIdf = pd.DataFrame()
    for QIf in QIflist:
        QIdf = QIdf.append(pd.read_csv(QIf), ignore_index=True)
    QIdf['prdate'] = [''.join(i.split('_')[-2:]) for i in QIdf['id']]
    dfMeta['prdate'] = [''.join(i.split('_')[2:4]) for i in dfMeta['LANDSAT_PRODUCT_ID']]
    subDf = dfMeta.loc[dfMeta['prdate'].isin(QIdf['prdate'])]
    subDf = subDf.drop_duplicates('LANDSAT_PRODUCT_ID')
    f = open(path.join(thumb_root, 'urlThumbD.csv'), 'w')
    for url in subDf.browseURL:
        wrsPath = url.split('/')[6]
        wrsrow = url.split('/')[7]
        opath = path.join(thumb_root, wrsPath, wrsrow)
        f.write(url+'\n')
        f.write('\t'+'dir='+ opath+'\n')
