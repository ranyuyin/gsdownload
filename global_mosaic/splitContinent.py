import pandas as pd
from glob import glob
from os import path
import os
from tqdm import tqdm
import shutil
prDict = {'as': r"Z:\yinry\global_mosaic\0.def\PR_Asia.csv",
          'af': r"Z:\yinry\global_mosaic\0.def\PR_Africa.csv",
          'oc': r"Z:\yinry\global_mosaic\0.def\PR_Oceania.csv",
          'eu': r"Z:\yinry\global_mosaic\0.def\PR_Europe.csv",
          'nam': r"Z:\yinry\global_mosaic\0.def\PR_NorthAmerica.csv",
          'sam': r"Z:\yinry\global_mosaic\0.def\PR_SouthAmerica.csv"}

if __name__ == '__main__':
    reproot =r'Z:\yinry\global_mosaic\2005\4.rep'
    pixall = glob(path.join(reproot, '*.pix'))
    pixdf = pd.DataFrame(data={'fpath': pixall})
    pixdf['pr'] = [int(path.basename(i).split('_')[2])  for i in pixdf['fpath']]
    for key, prdef in prDict.items():
        subpr = pd.read_csv(prdef)
        subdir = path.join(reproot, key)
        if not path.exists(subdir):
            os.mkdir(subdir)
        subdf = pixdf.loc[pixdf.pr.isin(subpr.PR)]
        for fp in tqdm(subdf.fpath):
            shutil.move(fp, subdir)
