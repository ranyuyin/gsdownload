import down_util
import pandas as pd

if __name__=='__main__':
    df,_=down_util.split_collection(r"D:\DATA\GOOGLE\landsat_index.csv.gz")
    prlist = pd.read_csv('cnpr.txt').PR
    sta_table = r'C:\Users\ranyu\Desktop\half_year_count_50.csv'
    # testprlist=['121032','121033','020020']
    down_util.seasonal_count(df, sta_table, todopr=prlist, date_split=('1-1', '7-1'), year_start=1986, year_end=2018, cloudlt=50)