import pandas as pd
import shutil
from glob import glob
from os import path
from tqdm import tqdm

if __name__ == '__main__':
    defcsv = r'Z:\yinry\global_mosaic\0.def\CR180PR.csv'
    reppath = r'Z:\yinry\global_mosaic\2011\4.rep'
    backdir = r'Z:\yinry\global_mosaic\2011\4.rep.back'
    crossdefdf = pd.read_csv(defcsv, dtype={'PR': str})
    crossdefdf['path'] = [i[:-3] for i in crossdefdf.PR]
    crossdefdf['row'] = [i[-3:] for i in crossdefdf.PR]
    fpd = pd.DataFrame(data={'fpath': glob(path.join(reppath, '*.pix'))})
    fpd['id'] = [path.basename(i).split('4mosaic')[-1] for i in fpd.fpath]
    dofpd = fpd.loc[fpd.id != '.pix']
    dofpd['id'] = [i[0] for i in dofpd.id]
    dofpd['PR'] = [str(int(path.basename(i).split('_')[2])) for i in dofpd.fpath]
    dofpd['row'] = [i[-3:] for i in dofpd.PR]
    dofpd['fsize'] = [path.getsize(i) for i in dofpd.fpath]
    dofpd = dofpd.sort_values(by='fsize')
    movelist = []
    for doid in ('1', '2'):
        thisdf = dofpd.loc[dofpd.id == doid]
        for name, group in thisdf.groupby('row'):
            if len(group.PR.unique()) < 2:
                continue
            else:
                movelist += list(group.iloc[:-1].fpath)
    for fpath in tqdm(movelist):
        shutil.move(fpath, backdir)