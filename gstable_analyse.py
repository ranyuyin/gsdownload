import pandas as pd
import numpy as np
from glob import glob
from os import path
import os
import matplotlib.pyplot as plt

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
    data_xlsx.WRS_PATH=data_xlsx.WRS_PATH.astype(np.int64)
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
    df.reset_index(inplace=True)
    year_month_list = [(item.year, item.month) for item in df.DATE_ACQUIRED]
    year_month_pd = pd.DataFrame(year_month_list, columns=['year', 'month'])
    return df.join(year_month_pd)

def Get_zone(df,lat,year,months=None):
    df=preprocess(df,year)
    df = df.loc[((df.NORTH_LAT >=lat[0]) & (df.NORTH_LAT < lat[1]))]
    if months is not None:
        df=df.loc[df.month.isin(months)]
    df = df.sort_values(by=['CLOUD_COVER', 'COLLECTION_CATEGORY'])
    adopt=df.loc[df.duplicated(['WRS_PATH','WRS_ROW'])==False]
    return adopt
def write_urls(df,dstdir,filename):
    df.BASE_URL.to_csv(path.join(dstdir,filename), index_label=False, index=False)
def preprocess(df,year=2016):
    if year>=2003:
        sub = df.loc[df.SPACECRAFT_ID != 'LANDSAT_7']
    #夜间的Landsat影像云量为-1
    sub = sub.loc[sub.CLOUD_COVER >= 0]
    #选取指定年份
    sub = addyearmonth(sub)
    sub=sub.loc[sub.year==year]
    return sub

def zone_download(year,dstdir):
    if not path.exists(dstdir):
        os.mkdir(dstdir)
    filename = r"D:\download_work\index.csv"
    ldst_df = pd.read_csv(filename,
                          usecols=['SCENE_ID', 'SENSOR_ID', 'SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                   'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL',
                                   'NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])

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
        df = Get_zone(ldst_df, lat, year,month)
        write_urls(df,dstdir,fname)


    # tropics_subl8=tropics_sub.loc[tropics_sub.SPACECRAFT_ID=='LANDSAT_8']
    # tropics_subl8_2016 = tropics_subl8_2016.sort_values(by=['CLOUD_COVER', 'COLLECTION_CATEGORY'])
    # adopt_tropic=tropics_subl8_2016.loc[tropics_subl8_2016.duplicated(['WRS_PATH','WRS_ROW'])==False]
    # adopt_tropic.BASE_URL.to_csv(r"C:\Users\ranyu\Desktop\tropic2016_l8.csv",index_label=False, index=False)


if __name__ == '__main__':
    dirpath = u'C:\\Users\\ranyu\\Desktop\\A类先导\\DATA_TABLE'
    filename = r"C:\Users\ranyu\Desktop\index.csv"
    ldst_df = pd.read_csv(filename, usecols=['SCENE_ID', 'SENSOR_ID', 'SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                             'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL','NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])
    allrecord = mergedir(dirpath)
    inter=pd.merge(allrecord, ldst_df)
    shortScene_ID = [this[:16] for this in inter.SCENE_ID]
    inter['shortID'] = shortScene_ID
    rmshortnamedupchina = inter.loc[inter.duplicated(subset='shortID') == False]
    adopt = rmshortnamedupchina.loc[rmshortnamedupchina.CLOUD_COVER >= 0]
    # plt.hist(adopt.CLOUD_COVER, bins=10)
    chinaOnly=allrecord.loc[~allrecord.ID.isin(adopt.ID)]
    print(allrecord.shape)