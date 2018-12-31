#coding:utf-8
# Import all libraries needed for the tutorial

# General syntax to import specific functions in a library:
##from (library) import (specific library function)
from pandas import DataFrame, read_csv
import os
import numpy as np
# General syntax to import a library but no functions:
##import (library) as (give the library a nickname/alias)
import matplotlib.pyplot as plt
import pandas as pd #this is how I usually import pandas
import sys #only needed to determine Python version number
import matplotlib #only needed to determine Matplotlib version number
#Location = r'C:\Users\ranyu\Desktop\index.csv'
#df = pd.read_csv(Location,usecols=['SCENE_ID','SENSOR_ID','SPACECRAFT_ID','COLLECTION_NUMBER','COLLECTION_CATEGORY','WRS_PATH','WRS_ROW','CLOUD_COVER','BASE_URL'],dtype={'COLLECTION_NUMBER':str})
#l5sub=df.loc[(df.SPACECRAFT_ID=='LANDSAT_5')&(df.SENSOR_ID=='TM')&(df.COLLECTION_NUMBER=='01')&(df.COLLECTION_CATEGORY=='T1')]
#打开数据表
workdir=r"C:\Users\ranyu\Desktop\download_work"
Location = os.path.join(workdir,r'index.csv')
df = pd.read_csv(Location,usecols=['SCENE_ID','SENSOR_ID','SPACECRAFT_ID','DATE_ACQUIRED','COLLECTION_NUMBER',\
                                   'COLLECTION_CATEGORY','WRS_PATH','WRS_ROW','CLOUD_COVER','BASE_URL'],\
                 dtype={'COLLECTION_NUMBER':str})
#获取所有L8数据：
l8sub=df.loc[(df.SPACECRAFT_ID=='LANDSAT_8')&(df.COLLECTION_NUMBER=='01')&(df.COLLECTION_CATEGORY=='T1')]
#生成年份
year=[this.year for this in pd.to_datetime(l8sub.DATE_ACQUIRED)]
l8sub['year']=year
l8sub2015=l8sub.loc[l8sub.year==2015]
#初始化结果列表
todoscene=pd.DataFrame()
#打开查询文件
scfile=open(r"C:\Users\ranyu\Desktop\download_work\buu2.txt").read()
PRs=pd.DataFrame(scfile.decode('GBK').split(u'、'),columns=['PR'])
#剔除已有数据
existPR=pd.read_csv(r"C:\Users\ranyu\Desktop\download_work\exist_wang.txt")
existPR=pd.DataFrame([this.split('_')[2] for this in existPR.ID],columns=['PR'])
PRs=PRs.loc[~PRs.PR.isin(existPR.PR)]
PRs=PRs.PR.unique()
PRs.sort()
#遍历轨道号
for item in PRs.PRS.get_values():
    thisPath=item[:3]
    thisRow=item[3:]
    PRmatchscenes=l8sub2015.loc[(l8sub2015.WRS_PATH==int(thisPath))&(l8sub2015.WRS_ROW==int(thisRow))]
    if len(PRmatchscenes)==0:
        continue
    PRmatchscenes=PRmatchscenes.loc[PRmatchscenes.CLOUD_COVER>=0].sort_values(['CLOUD_COVER'])
    todoscene=todoscene.append(PRmatchscenes.iloc[0])
todoscene.BASE_URL.to_csv(r'C:\Users\ranyu\Desktop\download_work\l8wang_url1.csv',index=False,index_label=False)

prlist=pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\china.path.raw.csv')
l5index=pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\l5index_T1.csv')
filelistfold=r'C:\Users\ranyu\Desktop\download_work\prlist'
todofolder=r'C:\Users\ranyu\Desktop\download_work\tododown(15,20]'
#下载（15，20]
for i in range(len(prlist)):
    thispr=l5index.loc[(l5index['WRS_PATH']==prlist['PATH'][i])&(l5index['WRS_ROW']==prlist['ROW'][i])]
    #todo:自定义云量
    thispr=thispr.loc[(thispr.CLOUD_COVER>15)&(thispr.CLOUD_COVER<=20)]
    listname2r=os.path.join(filelistfold,str(prlist['PATH'][i]).zfill(3)+str(prlist['ROW'][i]).zfill(3)+'.txt')
    if os.path.exists(listname2r):
        scenelist=open(listname2r).readlines()
        scenelist=[os.path.splitext(os.path.splitext(os.path.basename(line))[0])[0] for line in scenelist]
        todolist=thispr.loc[~thispr.SCENE_ID.isin(scenelist)]
    else:
        todolist=thispr
    todolist.BASE_URL.to_csv(os.path.join(todofolder,str(prlist['PATH'][i]).zfill(3)+str(prlist['ROW'][i]).zfill(3)+'.txt'),index_label=False,index=False)
    # 下载（20，50]
    prlist = pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\china.path.raw.csv')
    l5index = pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\l5index_T1.csv')
    filelistfold = r'C:\Users\ranyu\Desktop\download_work\prlist'
    todofolder = r'C:\Users\ranyu\Desktop\download_work\tododown(20,50]'
    # 下载（15，20]
    for i in range(len(prlist)):
        thispr = l5index.loc[(l5index['WRS_PATH'] == prlist['PATH'][i]) & (l5index['WRS_ROW'] == prlist['ROW'][i])]
        # todo:自定义云量
        thispr = thispr.loc[(thispr.CLOUD_COVER > 20) & (thispr.CLOUD_COVER <= 50)]
        listname2r = os.path.join(filelistfold,
                                  str(prlist['PATH'][i]).zfill(3) + str(prlist['ROW'][i]).zfill(3) + '.txt')
        if os.path.exists(listname2r):
            scenelist = open(listname2r).readlines()
            scenelist = [os.path.splitext(os.path.splitext(os.path.basename(line))[0])[0] for line in scenelist]
            todolist = thispr.loc[~thispr.SCENE_ID.isin(scenelist)]
        else:
            todolist = thispr
        todolist.BASE_URL.to_csv(
            os.path.join(todofolder, str(prlist['PATH'][i]).zfill(3) + str(prlist['ROW'][i]).zfill(3) + '.txt'),
            index_label=False, index=False)

        #重新下载网络中心已有数据