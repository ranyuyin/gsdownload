import down_util
from os import path
import pandas as pd
if __name__=='__main__':
    filename = r"D:\PROJECT\A_Project\global_download\index.csv.gz"
    ldst_df = pd.read_csv(filename,
                          usecols=['SCENE_ID', 'SENSOR_ID', 'SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                   'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL',
                                   'NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])

    years=[2000]
    base_dir=r'D:\PROJECT\A_Project\global_download'
    for year in years:
        print(year)
        year_dir=path.join(base_dir,str(year))
        down_util.zone_download(ldst_df,year,year_dir)