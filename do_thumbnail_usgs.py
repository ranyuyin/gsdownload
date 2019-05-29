import down_util
import pandas as pd
import os
from glob import glob
from os import path

# os.environ['http_proxy']='127.0.0.1:1080'
# os.environ['https_proxy']='127.0.0.1:1080'


if __name__=='__main__':
    df, _ = down_util.split_collection(r"Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz")
    # pr_list_file = r"Z:\yinry\global_mosaic\2018.need.1.pidlist.txt"
    # condi_df = down_util.condi_thumbnail_by_pr(df, pr_list_file, '2017-10-01', '2018-12-31', 20, mode='PID')
    # condi_df.to_csv(r'Z:\yinry\global_mosaic\2018.thumb.need.1\condi_df.csv')
    # condi_df = pd.read_csv(r'Z:\yinry\global_mosaic\2018.thumb.need.1\condi_df.csv')
    # down_util.download_c1df_thumbnail(condi_df,
    #                                   r'D:\PROJECT\A_Project\global_download\2000\cloud2000.thumbnail')

    pr_lists = [r"Z:\yinry\china.mosaic\china.pr.txt"]
    down_root = r'Z:\yinry\china.mosaic\1990\thumbnail'
    ref_root = r'Z:\yinry\global_mosaic\global_thumbnail_ref\global_thumbnail_0203'
    # condi_df_list = []
    #
    # for i, one_pr_list in enumerate(pr_lists):
    #     # condi_df_list.append([])
    startdate = '1990-03-01'
    enddate = '1990-11-30'
    thumb_root = r'Z:\yinry\china.mosaic\1990\thumbnail'
    #     this_condi = down_util.condi_thumbnail_by_pr(df, one_pr_list, startdate, enddate, 20, mode='PR')
    #     # this_down_root = path.join(down_root, str(year))
    #     down_util.download_c1df_thumbnail(this_condi, down_root)
    good = down_util.BestsceneWoker(ref_root, pr_lists[0], startdate, enddate, thumb_root, copydir=r'Z:\yinry\china.mosaic\1990\good')
    pd.DataFrame(data={'good': good}).to_csv(r'Z:\yinry\china.mosaic\1990\thumbnail\good.csv', index=False, header=False)
    exit(0)
