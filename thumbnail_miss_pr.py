from os import path
import pandas as pd
from glob import glob
from down_util import pr_from_pid

if __name__ == '__main__':
    pr_china = r"Z:\yinry\china.mosaic\china.pr.txt"
    check_dir = r''
    pr_china = pd.read_csv(pr_china, header=None, names=['PR'], dtype={'PR': str})
    check_list = glob(path.join(check_dir, '*.jpg'))
    existpr = [pr_from_pid(path.basename(i)) for i in check_list]
    existdf = pd.DataFrame(data={'PR': existpr})
    misspr = pr_china.loc[~pr_china['PR'].isin(existdf['PR'])]
    print(list(misspr['PR']))
