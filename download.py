
#coding:utf-8
import os
#import pandas as pd
from glob import glob
import matplotlib.pyplot as plt
#prlist=pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\china.path.raw.csv')
#l5index=pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\l5index.csv')
#filelistfold=r'C:\Users\ranyu\Desktop\download_work\prlist'
#todofolder=r'C:\Users\ranyu\Desktop\download_work\tododown'
downloaddir=r'H:\LANDSAT_5'
filelist=glob(r'C:\Users\ranyu\Desktop\download_work\tododown\*.txt')
# def count(l5index,prlist,step=5):
#     allpr=pd.DataFrame()
#     for i in range(len(prlist)):
#         thispr = l5index.loc[(l5index['WRS_PATH'] == prlist['PATH'][i]) & (l5index['WRS_ROW'] == prlist['ROW'][i])]
#         # todo:自定义云量
#         allpr=allpr.append(thispr,ignore_index=True)
#     binlow=range(0,100,step)
#     binhigh=range(step,100+step,step)
#     bins=[]
#     for i in range(len(binlow)):
#         bins.append(len(allpr.loc[(allpr.CLOUD_COVER < binhigh[i])&(allpr.CLOUD_COVER >= binlow[i])]))
#     return bins
def get_dirname(URL,dstroot):
    return os.path.join(dstroot,os.path.split(URL)[-1])
for file in filelist:
    f=open(file)
    urls=f.readlines()
    f.close()
    pr=os.path.splitext(os.path.basename(file))[0]
    dstdir=os.path.join(downloaddir,pr[:3],pr[3:])
    if not os.path.exists(dstdir):
        os.makedirs(dstdir)
    for url in urls:
        #os.system('set http_proxy=127.0.0.1:1080')
        #print 'gsutil cp -r {0} {1}'.format(url.strip('\n')+'/',dstdir)
        os.system('gsutil -m cp -r -n {0} {1}'.format(url.strip('\n')+'/',dstdir))
    os.remove(file)

