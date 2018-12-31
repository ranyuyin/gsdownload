# coding:utf-8
import os, sys
# import pandas as pd
from glob import glob
from os import path



def parse_url(url, craft='LANDSAT'):
    if craft == 'LANDSAT':
        basedir,product_id=path.split(url)
        basedir,orbit_row=path.split(basedir)
        basedir,orbit_path=path.split(basedir)
        return orbit_path,orbit_row
    if craft == 'SENTINEL2':
        basedir, product_id = path.split(url)
        basedir, t2 = path.split(basedir)
        basedir, t1 = path.split(basedir)
        basedir, zone = path.split(basedir)
        return zone, t1, t2


def downloadone(file, downloaddir=None, craft='LANDSAT', wildchar=''):
    urls = open(file).readlines()
    if not path.exists(downloaddir):
        os.makedirs(downloaddir)
    for i in range(len(urls)):
        url = urls.pop()
        # os.system('set http_proxy=127.0.0.1:1080')
        # print 'gsutil cp -r {0} {1}'.format(url.strip('\n')+'/',dstdir)
        if craft=='LANDSAT':
            orbit_path, orbit_row = parse_url(url)
            scenedir = path.join(downloaddir, orbit_path, orbit_row)
        if craft=='SENTINEL2':
            zone, t1, t2 = parse_url(url, craft)
            scenedir = path.join(downloaddir, zone, t1, t2)
        if not path.exists(scenedir):
            os.makedirs(scenedir)
        while (os.system('gsutil -m cp -r -n {0} {1}'.format(url.strip('\n') + '/'+wildchar, scenedir)) != 0):
            pass
        open(file, 'w').writelines(urls)

if __name__ == '__main__':
    tododown = sys.argv[1]
    downloaddir = sys.argv[2]
    craft = sys.argv[3]
    if len(sys.argv)>4:
        wildchar = sys.argv[4]
    else:
        wildchar = ''
    if wildchar is None:
        wildchar = ''
    if os.path.isdir(tododown):
        filelist = glob(os.path.join(tododown, r'*.txt'))
        for file in filelist:
            downloadone(file, downloaddir, craft, wildchar)
            # urls = open(file).readlines()
            # if not path.exists(downloaddir):
            #     os.makedirs(downloaddir)
            # for i in range(len(urls)):
            #     url = urls.pop()
            #     # os.system('set http_proxy=127.0.0.1:1080')
            #     # print 'gsutil cp -r {0} {1}'.format(url.strip('\n')+'/',dstdir)
            #     orbit_path,orbit_row=parse_url(url)
            #     scenedir=path.join(downloaddir,orbit_path,orbit_row)
            #     if not path.exists(scenedir):
            #         os.makedirs(scenedir)
            #     while (os.system('gsutil -m cp -r -n {0} {1}'.format(url.strip('\n') + '/', scenedir)) != 0):
            #         pass
            #     open(file, 'w').writelines(urls)
    else:
        downloadone(tododown, downloaddir, craft, wildchar)
        # urls = open(tododown).readlines()
        # if not os.path.exists(downloaddir):
        #     os.makedirs(downloaddir)
        # for i in range(len(urls)):
        #     # os.system('set http_proxy=127.0.0.1:1080')
        #     # print 'gsutil cp -r {0} {1}'.format(url.strip('\n')+'/',dstdir)
        #     url=urls.pop()
        #     orbit_path, orbit_row = parse_url(url)
        #     scenedir = path.join(downloaddir, orbit_path, orbit_row)
        #     if not path.exists(scenedir):
        #         os.makedirs(scenedir)
        #     while(os.system('gsutil -m cp -r -n {0} {1}'.format(url.strip('\n') + '/', scenedir))!=0):
        #         pass
        #     open(tododown,'w').writelines(urls)
