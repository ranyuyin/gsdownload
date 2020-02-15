# Quick Scene Analyse for quick view (thumbnail)

import rasterio as rio
import sklearn as skl
import skimage as ski
import pandas as pd, numpy as np
import down_util as dutil
from os import path
from datetime import datetime


class SAWorker:
    indexDf = None
    prmDf = None  # start month & end month definition of each path/row

    def __init__(self, index_db, prmdef):
        self.indexDf, _ = dutil.split_collection(index_db)
        self.prmDf = pd.read_csv(prmdef)

    def SASingle(self, p_thumb):
        datepr = dutil.datepr_from_pid(path.basename(p_thumb))
        date = datetime(year=int(datepr[:4]), month=int(datepr[4:6]), day=int(datepr[6:8]))
        wrs2path, wrs2row = int(datepr[8:11]), int(datepr[11:14])
        pr = str(int(datepr[8:14]))
        # calc cloud cover scores
        ccs = 1-self.indexDf.loc[(self.indexDf['DATE_ACQUIRED'] == date) &
                              (self.indexDf['WRS_PATH'] == wrs2path) & (self.indexDf['WRS_ROW'] == wrs2row)][
            'CLOUD_COVER'][0]/100

        # calc date scores
        # doy = date.timetuple().tm_yday
        prm = self.prmDf.loc[self.prmDf['PR'] == pr]
        m_start, m_end = prm['start_mon'][0], prm['end_mon'][0]
        m_end = m_end + 12 if m_end < m_start else m_end
        m_mid = (m_start + m_end) / 2
        m_mid = m_mid % 12 if (m_mid % 12) != 0 else 12
        aimdate = datetime(year=int(datepr[:4]), month=int(m_mid), day=15)
        # input of gaussian (x)
        delta = (date - aimdate).days
        if delta > 0:
            delta = delta % 183
        else:
            delta = delta % -183
        sig = 15 * (m_end - m_start)  # 30*(m_end - m_start)/2
        ds = gaussian(delta, 0, sig)



def gaussian(x, mu, sig):
    return np.exp(-np.power(x - mu, 2.) / (2 * np.power(sig, 2.)))


if __name__ == '__main__':
    pass
