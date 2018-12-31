import pandas as pd
import numpy as np
from glob import glob
from os import path
import os
import matplotlib.pyplot as plt
import down_util

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