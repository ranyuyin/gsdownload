import down_util
import pandas as pd
if __name__=='__main__':
    df,_=down_util.split_collection(r"D:\DATA\GOOGLE\landsat_index.csv.gz")
    PIDlist=pd.read_csv(r"D:\PROJECT\A_Project\global_download\2000\2000.select.pid.1220.txt", header=None,
                        names=['PID'])
    print('num: ', len(PIDlist))
    todownload=down_util.Get_zone(df, todoPID=PIDlist.PID, L7=True)
    todownload.BASE_URL.to_csv(r"D:\PROJECT\A_Project\global_download\2000\2000.select.url.1220.csv",
                               index=False)
