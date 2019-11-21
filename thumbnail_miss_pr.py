from os import path
import pandas as pd
from glob import glob
from down_util import pr_from_pid

if __name__ == '__main__':
    pr_china = r"Z:\yinry\china.mosaic\china.pr.txt"
    # pr_china = r"Z:\yinry\global_mosaic\0.def\prwithrange.csv"
    check_dir = r'Z:\yinry\china.mosaic\1986\4.rgb'
    pr_china = pd.read_csv(pr_china, dtype={'PR': str})
    check_list = glob(path.join(check_dir, '*.tif'))
    existpr = [str(int(path.basename(i)[:6])) for i in check_list]
    # existpr = [pr_from_pid(path.basename(i)) for i in check_list]
    existdf = pd.DataFrame(data={'PR': existpr})
    misspr = pr_china.loc[~pr_china['PR'].isin(existdf['PR'])]
    misspr.to_csv(r'Z:\yinry\china.mosaic\1986\4.rgb\missing1030.csv', index=False)
