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
    parser.add_argument('-f', help='list file', dest='listfile', action='store_true')
    parser.add_argument('-r', help='includeRT', dest='useRT', action='store_true')
    args = parser.parse_args()
    if args.listfile:
        jpglist = [path.basename(i.strip()) for i in open(args.thmdir).readlines()]
    else:
        jpglist = glob(path.join(args.thmdir, '*.jpg'))
        jpglist = [path.basename(i) for i in jpglist]
    df, _ = down_util.split_collection(args.gstable)
    df = down_util.Get_zone(df, todoPID=jpglist)
    if args.ourllist is not '':
        ourlpath = args.ourllist
    else:
        odir, oname = path.split(args.thmdir)
        ourlpath = path.join(odir, oname + '_url.txt')
    if not args.useRT:
        df = df.loc[df.COLLECTION_CATEGORY != 'RT']
    df.BASE_URL.to_csv(ourlpath, index=False, header=False)
