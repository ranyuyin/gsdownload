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
    # # down_util.download_c1df_thumbnail(condi_df,
    # #                                   r'D:\PROJECT\A_Project\global_download\2000\cloud2000.thumbnail')

    # pr_lists = [r"Z:\yinry\global_mosaic\2018.need.1.pidlist.txt"]
    down_root = r'Z:\yinry\global_mosaic\2018.thumb.all'
    # pixtxt = r"Z:\yinry\global_mosaic\2018.rep\2018list.txt"
    # pixlist = pd.read_csv(pixtxt, header=None, names=['pixname'])
    # dateprlist = down_util.pixfilelist2datepr(pixlist.pixname)
    # condi_df = down_util.Get_zone(df, tododatepr=dateprlist)
    # down_util.download_c1df_thumbnail(condi_df, down_root)
    pidtxt = r"Z:\yinry\global_mosaic\2018.thumb.all\pid.txt"
    pixlist = pd.read_csv(pidtxt, header=None, names=['PID'])
    dateprlist = down_util.pidlist2datepr(pixlist.PID)
    condi_df = down_util.Get_zone(df, tododatepr=dateprlist)
    down_util.download_c1df_thumbnail(condi_df, down_root)
    exit(0)
