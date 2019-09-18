# 根据缩略图列表获取gs-url
import down_util, argparse
from glob import glob
from os import path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='jpg2url')
    parser.add_argument('-j', help='a dir include thumbnails to download', dest='thmdir', required=True)
    parser.add_argument('-t', help='path google cloud index.csv', dest='gstable',
                        required=False, default=r'Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz')
    parser.add_argument('-o', help='output url list file', dest='ourllist', required=False, default='')
    args = parser.parse_args()
    jpglist = glob(path.join(args.thmdir, '*.jpg'))
    df, _ = down_util.split_collection(args.gstable)
    jpglist = [path.basename(i) for i in jpglist]
    df = down_util.Get_zone(df, todoPID=jpglist)
    if args.ourllist is not '':
        ourlpath = args.ourllist
    else:
        odir, oname = path.split(args.thmdir)
        ourlpath = path.join(odir, oname + '_url.txt')
    df.BASE_URL.to_csv(ourlpath, index=False, header=False)
