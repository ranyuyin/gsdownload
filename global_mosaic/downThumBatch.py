import down_util
import argparse
import pandas as pd
from os import path, mkdir
from functools import partial
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool

def urlParser(row):
    dirmaps = {'LANDSAT_1': 'landsat_mss_c1',
               'LANDSAT_2': 'landsat_mss_c1',
               'LANDSAT_3': 'landsat_mss_c1',
               'LANDSAT_4': 'landsat_mss_c1',
               'LANDSAT_5': 'landsat_tm_c1',
               'LANDSAT_7': 'landsat_etm_c1',
               'LANDSAT_8': 'landsat_8_c1'}
    baseurl = 'https://ims.cr.usgs.gov/browse/'
    wrs_path = str(row['WRS_PATH']).zfill(3)
    wrs_row = str(row['WRS_ROW']).zfill(3)
    pid = row['PRODUCT_ID']
    craft_id = row['SPACECRAFT_ID']
    year = pid.split('_')[3][:4]
    thumbnail_url = baseurl + '/'.join([dirmaps[craft_id], year, wrs_path, wrs_row, pid + '.jpg'])
    return thumbnail_url

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get thumbnails by pr')
    parser.add_argument('-s', help='start date', dest='start_date', required=True)
    parser.add_argument('-e', help='end date', dest='end_date', required=True)
    parser.add_argument('-w', help='work dir', dest='work_dir', required=True)
    parser.add_argument('-d', help='path row definition', dest='prs',
                        required=False, default=r"Z:\yinry\global_mosaic\0.def\prwithrange.csv")
    parser.add_argument('-i', help='index file', dest='gstable',
                        required=False, default=r'Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz')
    parser.add_argument('-m', help='multi process', dest='nMulti', required=False, default=15, type=int)
    args = parser.parse_args()
    datepaser = '%Y-%m-%d'
    date_start, date_end = datetime.strptime(args.start_date, datepaser), datetime.strptime(args.end_date, datepaser)
    df, _ = down_util.split_collection(args.gstable)
    df = df.loc[(df.SPACECRAFT_ID != 'LANDSAT_7') | (df.DATE_ACQUIRED < datetime(year=2003, month=5, day=31))]
    prlist = pd.read_csv(args.prs, dtype={'PR': str})
    thumb_root = path.join(args.work_dir, '0.thumbnail')
    subDf = df.loc[(df.DATE_ACQUIRED > date_start) &
                       (df.DATE_ACQUIRED < date_end)]
    prDf = [str(int(str(i[0]) + str(i[1]).zfill(3))) for i in zip(subDf.WRS_PATH, subDf.WRS_ROW)]
    subDf['PR'] = prDf
    del df
    subDf = subDf.loc[subDf.PR.isin(prlist)]
    subDf.sort_values(by='PR')
    subDf['urlThumb'] = subDf.apply(urlParser, axis=1)
    subDf.to_csv(path.join(thumb_root, 'urlThumb.csv'), index=False)
