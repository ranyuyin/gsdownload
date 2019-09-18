from glob import glob
from os import path
import re
import down_util
import pandas as pd

def parser(fname, df):
    if len(path.basename(fname)) < 15:
        return 'GEE'
    else:
        nameparts = path.basename(fname).split('_')
        thisdp = nameparts[1] + nameparts[0]
        print(thisdp)
        match = df.loc[df.datepr == thisdp]
        print(match)
        return str(list(match.PRODUCT_ID)[0]).split('_')[1]

def pr(fname):
    return int(path.basename(fname)[:6])

if __name__ == '__main__':
    dig_dir = r'Z:\yinry\global_mosaic\2018\2018.rep'
    filelist = glob(path.join(dig_dir, '*.pix'))
    df, _ = down_util.split_collection(r"Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz")
    df = df.loc[(df.SPACECRAFT_ID == 'LANDSAT_8')]
    df = down_util.addyearmonth(df)
    df = df.loc[df.year >= 2017]
    datepr = [i[0].strftime('%Y%m%d') + str(i[1]).zfill(3) + str(i[2]).zfill(3) for i in
              zip(df.DATE_ACQUIRED, df.WRS_PATH, df.WRS_ROW)]
    df['datepr'] = datepr
    profile = [parser(fname, df) for fname in filelist]
    prlist = [pr(i) for i in filelist]
    outtable = pd.DataFrame()
    outtable['pr'] = prlist
    outtable['profile'] = profile
    outtable.to_csv(r'Z:\yinry\global_mosaic\2018\2018.rep\profile.txt')
