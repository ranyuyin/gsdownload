import down_util
from os import path,mkdir
import pandas as pd
if __name__=='__main__':
    filename = r"D:\download_work\index.csv"
    ldst_df = pd.read_csv(filename,
                          usecols=['SCENE_ID', 'SENSOR_ID', 'SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                   'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL',
                                   'NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])

    years=[2017]
    base_dir=r'D:\download_work'
    prlist=pd.read_csv(r'D:\download_work\path-row.txt')
    for year in years:
        print(year)
        year_dir=path.join(base_dir,'jiang_'+str(year))
        if not path.exists(year_dir):
            mkdir(year_dir)
        df = down_util.Get_zone(ldst_df, year,months=[6,7,8,9,10],inPathRows=prlist)
        down_util.write_subs(df, year_dir, str(year)+'all.csv')