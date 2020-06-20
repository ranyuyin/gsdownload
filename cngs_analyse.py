import pandas as pd
import numpy as np
from glob import glob
from os import path


def parser(ID_str):
    CRAFT, STATION, DTIME, PATH, ROW = ID_str.split('_')
    # DTIME=pd.to_datetime(DTIME,format="%Y-%m-%d")
    return CRAFT, STATION, DTIME[:10], int(PATH), int(ROW)


def parsexlsx(filename):
    # parsed=pd.DataFrame()
    print(filename)
    data_xlsx = pd.read_excel(filename, names=['ID'], usecols=[0])
    # parsed=data_xlsx.applymap(parser)
    a = [parser(item) for item in data_xlsx.ID]
    data_xlsx = pd.DataFrame(np.array(a), columns=['CRAFT', 'STATION', 'DATE', 'PATH', 'ROW'])
    return data_xlsx


def mergedir(dirpath):
    xlsxlist = glob(path.join(dirpath, '*.xlsx'))
    allrecord = pd.DataFrame()
    for item in xlsxlist:
        tem=parsexlsx(item)
        allrecord=allrecord.append(tem, ignore_index=True)
    return allrecord


if __name__ == '__main__':
    dirpath = r'Z:\yinry\0.DEFINITION\DATA_TABLE_CNGS\DATA_TABLE'
    allrecord = mergedir(dirpath)
    print(allrecord.shape)
    allrecord.to_csv(r'Z:\yinry\0.DEFINITION\DATA_TABLE_CNGS\DATA_TABLE\merged.csv', index=False)