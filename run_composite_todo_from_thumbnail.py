from os import path
from glob import glob
import re
from pandas import DataFrame

if __name__ == '__main__':
    badthumbdir = r'Z:\yinry\global_mosaic\2018\2018.thumb.all\bad'
    badthumblist = glob(path.join(badthumbdir, '*.jpg'))
    parten_cc = '^.{4,6}_.{4}_.{4}_(...)(...)_(.{4})(..)(..).*'
    # 需提取pr，根据pr生成合成缩略图
    prtodolist = [re.split(parten_cc, path.basename(i)) for i in badthumblist]
    prtodolist = [i[1]+i[2] for i in prtodolist]
    alltododf = DataFrame(data={'PR': prtodolist})
    # 提取已有的CC
    parten = '^.{4}_.{4}_(...)(...)_(.{4})(..)(..).*'
    first_round_list = glob(r'Z:\yinry\global_mosaic\2018\2018.thumb.need.1\*.tif')
    existpr_list = [re.split(parten, path.basename(i)) for i in first_round_list]
    existpr_list = [i[1]+i[2] for i in existpr_list]
    exist_df = DataFrame(data={'PR': existpr_list})
    final_pr = alltododf.loc[~alltododf['PR'].isin(exist_df['PR'])]
    final_pr.to_csv(r'Z:\yinry\global_mosaic\2018\2018.thumb.all\to_composite2.csv', index=Falseniz)