from matplotlib.pyplot import *
from glob import glob
from datetime import datetime
import matplotlib.pyplot as plt
from numpy.random import randn
from os import path
import pandas as pd
import numpy as np


def file_search_glob(inpath, condition):
    return glob(inpath + '\\'+condition)


def Get_rtrs(xlspath):
    allplot = pd.read_excel(xlspath, sheet_name='All Data')
    datatodo = allplot.iloc[:,12:20]  #逗号之前表示取哪一行，之后表示取那一列 多维数组都是按照这种方式来取数据的，影像加上波段就是三维数组 矩阵切片 array slice
    datatodo = np.array(datatodo)  # 这个玩意是空的。
    ABVs = datatodo[::6,:] #行方向上分隔成6个一组 列方向上都要
    BLWs = datatodo.reshape([-1,6,8])[:,1:5,:]  #reshape是改变矩阵的形状？后面那些代表什么意思
    mean_per_seg = BLWs.mean(axis=1)
    r_t_rs_per_seg = mean_per_seg/ABVs
    r_t_rs_per_seg_mean = r_t_rs_per_seg.mean(axis=1)  #axis=1代表什么
    [r_point, t_point, rs_point] = r_t_rs_per_seg_mean.reshape([-1,3]).transpose(1,0)
    [r_ESU, t_ESU, rs_ESU] = [i.reshape(-1,5).mean(axis=1) for i in [r_point, t_point, rs_point]]
    doy = parse(xlspath)
    FIPAR_ESU = 1-t_ESU
    FAPAR_ESU = 1 - r_ESU - t_ESU * (1 - rs_ESU)
    print(FIPAR_ESU.mean(), FAPAR_ESU.mean())
    return doy, r_ESU.mean(), t_ESU.mean(), rs_ESU.mean(), FIPAR_ESU.mean(), FAPAR_ESU.mean()

def parse(filepath):
    datestr = path.splitext(path.basename(filepath).split('_')[1])[0]
    thisdate = datetime.strptime(datestr,'%Y%m%d')
    doy = (thisdate - datetime(thisdate.year, 1, 1)).days + 1
    return doy

# inpath = r"F:\Hailun_experiment_20160519\Data\AccuPAR\diffusion"
# files = file_search_glob(inpath,"*.xls")
# data = np.array([Get_rtrs(i) for i in files])
# doy = data[:,0].astype('int')
# r = data[:,1]
# t = data[:,2]
# rs = data[:,3]
#
#
# with open(r"F:\Hailun_experiment_20160519\Data\AccuPAR\AccuPAR_data.txt", "w") as f:
#     f.write("%8s%8s%8s%8s\n" % ("doy", "ref", "trans", "ref_soil"))
#     for aa, bb, cc, dd in zip(doy, r, t, rs):
#         f.write("%s%8.2f%8.2f%8.2f\n" % (aa, bb, cc, dd))
#     f.close()

inpath = r"F:\Hailun_experiment_20160519\Data\AccuPAR\diffusion"
files = file_search_glob(inpath,"*.xls")
data = np.array([Get_rtrs(i) for i in files])
doy = data[:,0].astype('int')
FIPAR_ESU = data[:,4]
FAPAR_ESU = data[:,5]

with open(r"F:\Hailun_experiment_20160519\Data\AccuPAR\AccuPAR_FPAR.txt", "w") as f:
    f.write("%8s%8s%8s\n" % ("doy", "FIPAR_ESU", "FAPAR_ESU"))
    for aa, bb, cc in zip(doy, FIPAR_ESU, FAPAR_ESU):
        f.write("%s%8.2f%8.2f\n" % (aa, bb, cc))
    f.close()

fig = plt.figure()
ax = fig.add_subplot(3,2,1)
filename = r"F:\Hailun_experiment_20160519\Data\AccuPAR\plotA_FPAR.txt"
a, b, c = [], [], []
with open(filename, 'r') as f:#1
    lines = f.readlines()#2
    for line in lines:#3
        value = [float(s) for s in line.split()]#4
        print(value)
        a.append(value[0])#5
        b.append(value[1])
        c.append(value[2])

fig.set_size_inches([35 / 2.54, 30/ 2.54])

ax.plot(a, b, marker='o', color='b',linewidth=2)
ax.plot(a, c, marker='^', color='r',linewidth=2)
ax.tick_params(direction='in',which='both', top='on', right='on')

majorLocator = MultipleLocator(10)
majorFormatter = FormatStrFormatter('%d')
minorLocator = MultipleLocator(5)
ax.set_xlim(170, 270)

ax.set_xlabel('Day of year', fontsize='15')
ax.set_ylabel('FPAR', fontsize='15')
ax.text(172, 0.93, r'(a) Plot A (Maize)', ha='left', color='k', fontsize=13)
ax.legend(['FIPAR','FAPAR'],ncol=1,borderpad=0.2,handletextpad=0,columnspacing =1,handlelength =3,frameon =False,loc='lower right',fontsize=13)