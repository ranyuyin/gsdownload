from datetime import datetime
import pandas as pd

todoids = pd.read_csv(r"Z:\yinry\0315ids.csv")
todotimes = [datetime.strptime(oneid.split('_')[2],'%Y%m%dT%H%M%S') for oneid in todoids.ids]
todotiles = [oneid.split('_')[5] for oneid in todoids.ids]
tododatetiles = [onedate.strftime('%Y%m%d')+onetile for onedate,onetile in zip(todotimes, todotiles)]

s2_pd = pd.read_csv(r"Z:\yinry\index_sentinel.csv.gz", parse_dates=['SENSING_TIME'])
s2_pd_i = s2_pd.loc[s2_pd.MGRS_TILE.isin([i[1:] for i in todotiles])]
s2_pd_i['datetile'] = [onedate.strftime('%Y%m%d')+'T'+onetile for onedate, onetile in zip(s2_pd_i.SENSING_TIME, s2_pd_i.MGRS_TILE)]
select = s2_pd_i.loc[s2_pd_i.datetile.isin(tododatetiles)]
select.BASE_URL.to_csv(r'Z:\yinry\s2url.csv', index=False,header=False)