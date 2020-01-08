# Quick Scene Analyse for quick view (thumbnail)

import rasterio as rio
import sklearn as skl
import skimage as ski
import pandas as pd
import down_util as dutil
from os import path

class SAWorker:
    indexDf = None
    prmDf = None
    def __init__(self, index_db, prmdef):
        self.indexDf, _ = dutil.split_collection(index_db)
        self.prmDf = pd.read_csv(prmdef)

    def SASingle(self, p_thumb):
        cc = dutil.datepr_from_pid(path.basename(p_thumb))


if __name__ == '__main__':
    pass
