import down_util
import argparse
import pandas as pd
from os import path
from functools import partial
from datetime import datetime
from tqdm import tqdm
from multiprocessing import Pool

def doOne(pr, date_start, date_end, df, thumb_root, ignoreSLCoff=True, monthlist=(4,5,6,7,8,9,10), n_condi=25):
    wrs_path, wrs_row = int(pr[:3]), int(pr[3:])
    cach_candi = '{}_{}_{}_{}.csv'.format(str(wrs_path).zfill(3),
                                          str(wrs_row).zfill(3),
                                          datetime.strftime(date_start, '%Y%m%d'),
                                          datetime.strftime(date_end, '%Y%m%d'))
    cach_candi_path = path.join(thumb_root, cach_candi)
    if not path.exists(cach_candi_path):
        this_condi = down_util.Get_candi_by_onepr(wrs_path, wrs_row, date_start, date_end, df,
                                                  ignoreSLCoff, monthlist, n_condi)
        this_condi.to_csv(cach_candi_path)

    else:
        this_condi = pd.read_csv(cach_candi_path)
    candi_jpg_list = down_util.download_c1df_thumbnail(this_condi, thumb_root)
    print('{} has {} candi'.format(pr, len(candi_jpg_list)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get best scene automatically')
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
    prlist = pd.read_csv(args.prs, dtype={'PR': str})
    thumb_root = path.join(args.work_dir, '0.thumbnail')
    p = Pool(args.nMulti)
    for i in tqdm(p.imap(partial(doOne, date_start=date_start,
                                 date_end=date_end, df=df, thumb_root=thumb_root),
                         prlist.PR), total=len(prlist)):
        pass



