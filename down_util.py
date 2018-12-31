import pandas as pd
import numpy as np
from glob import glob
from os import path
import os
import re, wget
import matplotlib.pyplot as plt
from datetime import datetime
from tqdm import tqdm
from joblib import Parallel, delayed
import importlib # 可用于reload库


def count(l5index, prlist, step=5):
    allpr = pd.DataFrame()
    for i in range(len(prlist)):
        thispr = l5index.loc[(l5index['WRS_PATH'] == prlist['PATH'][i]) & (l5index['WRS_ROW'] == prlist['ROW'][i])]
        # todo:自定义云量
        allpr = allpr.append(thispr, ignore_index=True)
    binlow = range(0, 100, step)
    binhigh = range(step, 100 + step, step)
    bins = []
    for i in range(len(binlow)):
        bins.append(len(allpr.loc[(allpr.CLOUD_COVER < binhigh[i]) & (allpr.CLOUD_COVER >= binlow[i])]))
    return bins


def parser(ID_str):
    CRAFT, STATION, DTIME, PATH, ROW = ID_str.split('_')
    # DTIME=pd.to_datetime(DTIME,format="%Y-%m-%d")
    return CRAFT, STATION, DTIME[:10], int(PATH), int(ROW)


def parsexlsx(filename):
    # parsed=pd.DataFrame()
    data_xlsx_raw = pd.read_excel(filename, names=['ID','ULCLOUD','URCLOD','LLCLOUD','LRCLOUD'], usecols=[0,4,5,6,7])
    # parsed=data_xlsx.applymap(parser)
    a = [parser(item) for item in data_xlsx_raw.ID]
    data_xlsx = pd.DataFrame(np.array(a), columns=['CRAFT', 'STATION', 'DATE_ACQUIRED', 'WRS_PATH', 'WRS_ROW'])
    data_xlsx['DATE_ACQUIRED']=pd.to_datetime(data_xlsx.DATE_ACQUIRED)
    data_xlsx.WRS_PATH = data_xlsx.WRS_PATH.astype(np.int64)
    data_xlsx.WRS_ROW = data_xlsx.WRS_ROW.astype(np.int64)
    data_xlsx=data_xlsx.join(data_xlsx_raw)
    return data_xlsx


def mergedir(dirpath):
    xlsxlist = glob(path.join(dirpath, '*.xlsx'))
    allrecord = pd.DataFrame()
    for item in xlsxlist:
        tem=parsexlsx(item)
        allrecord=allrecord.append(tem,ignore_index=True)
    return allrecord


def df_lnst_pool(filename):
    df=pd.read_csv(filename,usecols=['SCENE_ID','SENSOR_ID','SPACECRAFT_ID','DATE_ACQUIRED','COLLECTION_NUMBER',
                                  'COLLECTION_CATEGORY','WRS_PATH','WRS_ROW','CLOUD_COVER','BASE_URL'],
                dtype={'COLLECTION_NUMBER': str},parse_dates=['DATE_ACQUIRED'])


def addyearmonth(df):
    df.reset_index(inplace=True, drop=True)
    year_month_list = [(item.year, item.month) for item in df.DATE_ACQUIRED]
    year_month_pd = pd.DataFrame(year_month_list, columns=['year', 'month'])
    return df.join(year_month_pd)


def Get_zone(df,year=None,lat=None ,months=None,inPathRows=None, exclude=None, todoPID=None, tododatepr=None, L7=False):
    df=preprocess(df, year, L7)
    PR = [int(str(i[0]) + str(i[1]).zfill(3)) for i in zip(df.WRS_PATH, df.WRS_ROW)]
    df['PR']=PR
    if lat is not None:
        df = df.loc[((df.NORTH_LAT >=lat[0]) & (df.NORTH_LAT < lat[1]))]
    if inPathRows is not None:
        df = df.loc[df['PR'].isin(inPathRows['PR'])]
    if months is not None:
        df=df.loc[df.month.isin(months)]
    if exclude is not None:
        todoPR=[int(i[0:6]) for i in exclude]
        df = df.loc[df['PR'].isin(todoPR)]
        prdate=[str(i[0]).zfill(3) + str(i[1]).zfill(3) + '_' + i[2].strftime('%Y%m%d') for i in zip(df.WRS_PATH, df.WRS_ROW, df.DATE_ACQUIRED)]
        df['prdate'] = prdate
        df = df.loc[~df['prdate'].isin(exclude)]
    if todoPID is not None:
        datepr = [i[0].strftime('%Y%m%d') + str(i[1]).zfill(3) + str(i[2]).zfill(3) for i in
                  zip(df.DATE_ACQUIRED,df.WRS_PATH, df.WRS_ROW)]
        df['datepr'] = datepr
        tododateprlist = [datepr_from_pid(i) for i in todoPID]
        df = df.loc[df['datepr'].isin(tododateprlist)]
        return df
    if tododatepr is not None:
        datepr = [i[0].strftime('%Y%m%d') + str(i[1]).zfill(3) + str(i[2]).zfill(3) for i in
                  zip(df.DATE_ACQUIRED, df.WRS_PATH, df.WRS_ROW)]
        df['datepr'] = datepr
        df = df.loc[df['datepr'].isin(tododatepr)]
        return df
    df = df.sort_values(by=['CLOUD_COVER'])
    adopt = df.loc[df.duplicated(['PR'])==False]
    return adopt


def write_subs(df,dstdir,filename,columns=['BASE_URL']):
    outframe=pd.DataFrame()
    for col in columns:
        outframe[col]=df[col]
    outframe.to_csv(path.join(dstdir, filename), index_label=False, index=False,header=False)


def preprocess(df, year=None, L7=False):
    df = df.loc[df.COLLECTION_NUMBER == '01']
    if year is not None:
        if year<2003:
            L7 = True
    if L7==False:
        sub = df.loc[df.SPACECRAFT_ID != 'LANDSAT_7']
    else:
        sub=df
    #夜间的Landsat影像云量为-1
    sub = sub.loc[sub.CLOUD_COVER >= 0]
    #选取指定年份
    sub = addyearmonth(sub)
    if year is not None:
        sub=sub.loc[sub.year==year]
    return sub


def zone_download(ldst_df, year, dstdir, columns=['BASE_URL'], PathRows = None, df_exclude = None):
    if not path.exists(dstdir):
        os.mkdir(dstdir)
    # tropics_sub=ldst_df.loc[(ldst_df.NORTH_LAT>-19)&(ldst_df.NORTH_LAT<20)]
    #去除云量-1的景
    # [40,90],[6,7,8]
    # [20,40),[5,6,7,8,9]
    # [-19,20),[all],
    # [-38,-19),[11,12,1,2,3]
    # [-90,-38),[12,1,2]
    lat_tuples=[(40, 92),
                (20, 40),
                (-19, 20),
                (-38, -19),
                (-90,-38)]
    month_tuples=[(6,7,8),
                  (5, 6, 7, 8, 9),
                  None,
                  (11,12,1,2,3),
                  (12,1,2)]

    name_tuples=['high_N.txt', 'mid_N.txt', 'tropic.txt', 'mid_S.txt', 'high_S.txt']
    for lat,month,fname in zip(lat_tuples,month_tuples,name_tuples):
        df = Get_zone(ldst_df, year, lat, month, PathRows, exclude = df_exclude)
        write_subs(df,dstdir,fname,columns)


    # tropics_subl8=tropics_sub.loc[tropics_sub.SPACECRAFT_ID=='LANDSAT_8']
    # tropics_subl8_2016 = tropics_subl8_2016.sort_values(by=['CLOUD_COVER', 'COLLECTION_CATEGORY'])
    # adopt_tropic=tropics_subl8_2016.loc[tropics_subl8_2016.duplicated(['WRS_PATH','WRS_ROW'])==False]
    # adopt_tropic.BASE_URL.to_csv(r"C:\Users\ranyu\Desktop\tropic2016_l8.csv",index_label=False, index=False)


def add_prefix(filename,prefix):
    return path.join(path.split(filename)[0], prefix+'_'+path.split(filename)[1])


# get landsat collection
def split_collection(filename, write=False):
    df = pd.read_csv(filename, usecols=['SCENE_ID', 'PRODUCT_ID', 'SENSOR_ID', 'SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                             'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL','NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])
    c1 = df.loc[df.COLLECTION_NUMBER=='01']
    cpre = df.loc[df.COLLECTION_NUMBER=='PRE']
    if write:
        c1.to_csv(add_prefix(filename,'c1'), index=False, compression='gzip')
        cpre.to_csv(add_prefix(filename,'cpre'), index=False, compression='gzip')
    return c1, cpre


def datepr_from_pid(pid):
    parten = '^.{4}_.{4}_(...)(...)_(.{4})(..)(..).*'
    _, wrs_path, wrs_row, year, month, day, _ = re.split(parten, pid)
    return datetime(int(year), int(month), int(day)).strftime('%Y%m%d') + wrs_path + wrs_row


def pr_from_pid(pid):
    parten = '^.{4}_.{4}_(...)(...)_(.{4})(..)(..).*'
    _, wrs_path, wrs_row, _, _, _, _ = re.split(parten, pid)
    return str(int(wrs_path))+wrs_row


def pr_list_from_pid_list_file(pidfile):
    dst_pr_list_file = addsuffix(pidfile, '.pr')
    PRdf = pd.DataFrame()
    PIDlist = pd.read_csv(pidfile, header=None, names=['PID'])
    PRdf['PR'] = PIDlist.PID.apply(pr_from_pid)
    PRdf.to_csv(dst_pr_list_file, index=False)


def addsuffix(fullpath, suffix):
    namepart = path.split(fullpath)
    fname, ext = path.splitext(namepart[1])
    return path.join(namepart[0],fname+suffix+ext)


def seasonal_count(df, sta_table, date_split=('1-1','4-1','7-1','10-1'), todopr=None,
                   year_start=1986, year_end=2018, cloudlt=20, encounter_slcoff=False):
    #SLC off date:2003-05-31
    df = df.loc[df.CLOUD_COVER>=0]
    PR = [int(str(i[0]) + str(i[1]).zfill(3)) for i in zip(df.WRS_PATH, df.WRS_ROW)]
    df['PR']=PR
    seasonlist = []
    percentilelist = []
    allpd_out = pd.DataFrame()

    if todopr is not None:
        df = df.loc[df.PR.isin(todopr)]
    if encounter_slcoff is not True:
        slc_off_date=datetime(2003,5,31)
        df = df.loc[(df.SPACECRAFT_ID != 'LANDSAT_7') | (df.DATE_ACQUIRED < slc_off_date)]
    for year in range(year_start,year_end+1):
        for i, i_start in enumerate(range(len(date_split)),1):
            date_start = datetime(year=year, month=int(date_split[i_start].split('-')[0]),
                                           day=int(date_split[i_start].split('-')[1]))
            i_end = (i_start+1)%len(date_split)
            year_end = year + (i_start+1)//len(date_split)
            date_end = datetime(year=year_end, month=int(date_split[i_end].split('-')[0]),
                                           day=int(date_split[i_end].split('-')[1]))

            thisdf = df.loc[(df.DATE_ACQUIRED>=date_start) & (df.DATE_ACQUIRED<date_end) & (df.CLOUD_COVER<=cloudlt)]
            this_date_splite_pd = pd.DataFrame(thisdf['PR'].groupby(thisdf['PR']).count())
            this_date_splite_pd.columns=[str(year)+'_'+str(i)]
            allpd_out = allpd_out.merge(this_date_splite_pd, how='outer', left_index=True, right_index=True)
            percentile = len(this_date_splite_pd.loc[this_date_splite_pd[str(year)+'_'+str(i)]>0])/len(todopr)
            seasonlist.append(str(year)+'_'+str(i))
            percentilelist.append(percentile)
    sta_out = pd.DataFrame(data={'year_season':seasonlist,'cover_percent':percentilelist})
    allpd_out = allpd_out.fillna(0)
    allpd_out.to_csv(sta_table)
    staname = path.splitext(sta_table)[0]+'_sta.csv'
    print(staname)
    sta_out.to_csv(staname)


def get_thumbnail_pid(pid, year, wrs_path, wrs_row, craft_id, download_root, errorlist):
    dirmaps = {'LANDSAT_1': 'landsat_mss_c1',
               'LANDSAT_2': 'landsat_mss_c1',
               'LANDSAT_3': 'landsat_mss_c1',
               'LANDSAT_4': 'landsat_mss_c1',
               'LANDSAT_5': 'landsat_tm_c1',
               'LANDSAT_7': 'landsat_etm_c1',
               'LANDSAT_8': 'landsat_8_c1'}
    baseurl = 'https://earthexplorer.usgs.gov/browse/'
    thumbnail_url = baseurl+'/'.join([dirmaps[craft_id], year, wrs_path, wrs_row, pid+'.jpg'])
    download_folder = path.join(download_root, wrs_path, wrs_row)
    while not path.exists(download_folder):
        try:
            os.makedirs(download_folder)
        except:
            pass
    try:
        down_file =  path.join(download_folder, pid+'.jpg')
        if not path.exists(down_file):
            wget.download(thumbnail_url, down_file)
        else:
            print('skip!')
    except:
        errorlist.append(pid)


def download_c1df_thumbnail(df, download_root):
    # df = addyearmonth(df)
    i = 0
    errorlist = []

    def download_row(row, errorlist):
        year = str(row.year)
        wrs_path = str(row.WRS_PATH).zfill(3)
        wrs_row = str(row.WRS_ROW).zfill(3)
        pid = row.PRODUCT_ID
        craft_id = row.SPACECRAFT_ID
        # print('downloading: ', pid)
        get_thumbnail_pid(pid, year, wrs_path, wrs_row, craft_id, download_root, errorlist)
        # print('done!')

    Parallel(n_jobs=8)(delayed(download_row)(row, errorlist) for row in tqdm(df.itertuples()))

    pd.DataFrame(data={'error_pid': errorlist}).to_csv(path.join(download_root, 'errorlist.csv'), index=False)


def condi_thumbnail_by_pr(df, pr_list_file, date_start, date_end, n_condi, datepaser='%Y-%m-%d', ignoreSLCoff=True):
    date_start, date_end = datetime.strptime(date_start, datepaser), datetime.strptime(date_end, datepaser)
    if 'year' not in df.columns and 'month' not in df.columns:
        df = addyearmonth(df)
    pr_m_list = pd.read_csv(pr_list_file, dtype={'PR': str})
    pd_condi = pd.DataFrame()
    if ignoreSLCoff:
        df = df.loc[(df.SPACECRAFT_ID!='LANDSAT_7')|(df.DATE_ACQUIRED<datetime(year=2003,month=5,day=31))]
    monthfilter = False
    if 'm_start' in pr_m_list.columns and 'm_end' in pr_m_list.columns:
        monthfilter = True
    i_record = 0
    for todopr_m in pr_m_list.itertuples():
        todopr = todopr_m.PR
        wrs_path = int(todopr[0:-3])
        wrs_row = int(todopr[-3:])
        thiscondi = df.loc[(df.WRS_PATH==wrs_path)&
                           (df.WRS_ROW==wrs_row)&
                           (df.DATE_ACQUIRED>date_start)&
                           (df.DATE_ACQUIRED<date_end)]
        if monthfilter:
            thiscondi = thiscondi.loc[
                (thiscondi.month - date_start) * (date_end - thiscondi.month) * (date_end - date_start)>0
            ]
        thiscondi = thiscondi.sort_values('CLOUD_COVER').head(n_condi)
        pd_condi = pd_condi.append(thiscondi)
        i_record+=1
        print('condi: {0}/{1})'.format(i_record, len(pr_m_list)))
    return pd_condi
        # pd_condi.append(thiscondi)
