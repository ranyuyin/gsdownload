import down_util
import pandas as pd
import os
from glob import glob
from os import path

# os.environ['http_proxy']='127.0.0.1:1080'
# os.environ['https_proxy']='127.0.0.1:1080'


if __name__=='__main__':
    df, _ = down_util.split_collection(r"D:\DATA\GOOGLE\landsat_index.csv.gz")
    # pr_list_file_ = r"D:\PROJECT\A_Project\global_download\2000\cloud.2000.to.select.pr.csv"
    # condi_df = down_util.condi_thumbnail_by_pr(df, pr_list_file, '1999-06-01', '2001-06-01', 20)
    # condi_df.to_csv(r'D:\PROJECT\A_Project\global_download\2000\cloud2000.thumbnail\condi_df.csv')
    # condi_df = pd.read_csv(r'D:\PROJECT\A_Project\global_download\2000\cloud2000.thumbnail\condi_df.csv')
    # down_util.download_c1df_thumbnail(condi_df,
    #                                   r'D:\PROJECT\A_Project\global_download\2000\cloud2000.thumbnail')
    pr_lists = glob(r'D:\PROJECT\A_Project\global_download\13-18.demo.area\*.csv')
    down_root = r'D:\PROJECT\A_Project\global_download\13-18.demo.area'
    # condi_df_list = []
    for i, one_pr_list in enumerate(pr_lists):
        # condi_df_list.append([])
        for year in range(2013,2019):
            startdate = '{}-01-01'.format(year)
            enddate = '{}-12-31'.format(year)
            this_condi = down_util.condi_thumbnail_by_pr(df, one_pr_list, startdate, enddate, 10)
            this_down_root = path.join(down_root, str(year))
            down_util.download_c1df_thumbnail(this_condi, this_down_root)
