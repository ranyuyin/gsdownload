import down_util, argparse
import pandas as pd
import os
from glob import glob
from os import path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get best scene automatically')
    parser.add_argument('-s', help='start date', dest='start_date', required=True)
    parser.add_argument('-e', help='end date', dest='end_date', required=True)
    parser.add_argument('-w', help='work dir', dest='work_dir', required=True)
    parser.add_argument('-d', help='path row definition', dest='prs',
                        required=False, default=r"Z:\yinry\global_mosaic\0.def\prwithrange.csv")

    parser.add_argument('-r', help='reference root', dest='ref_root',
                        required=False, default=r'Z:\yinry\global_mosaic\global_thumbnail_ref\global_thumbnail_0203')
    parser.add_argument('-i', help='index file', dest='gstable',
                        required=False, default=r'Z:\yinry\Landsat.Data\GOOGLE\landsat_index.csv.gz')
    parser.add_argument('-m', help='multi process', dest='nMulti', required=False, default=15, type=int)
    args = parser.parse_args()
    df, _ = down_util.split_collection(args.gstable)
    thumb_root = path.join(args.work_dir, '0.thumbnail')
    good = down_util.BestsceneWoker(args.ref_root, args.prs, args.start_date, args.end_date, thumb_root,
                                    copydir=path.join(args.work_dir, '1.good'), df=df, Global_monthlist=[4,5,6,7,8,9,10],
                                    nprocess=args.nMulti)
    pd.DataFrame(data={'good': good}).to_csv(path.join(thumb_root, 'good.csv'), index=False, header=False)
    exit(0)
