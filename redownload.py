import pandas as pd
import os
def getnocomplet(ferror,filename=r"D:\download_work\index.csv"):
    alldf = pd.read_csv(filename, usecols=['SCENE_ID', 'SENSOR_ID', 'PRODUCT_ID','SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                             'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL','NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])
    need_url_file=os.path.join(os.path.split(ferror)[0],'todo_no_complet.txt')
    error_df=pd.read_csv(ferror)
    interact=error_df.merge(alldf,on='PRODUCT_ID',how ='inner')
    interactPRE=error_df.merge(alldf,left_on='PRODUCT_ID',right_on='SCENE_ID')
    #todo:
    interact.append(interactPRE).BASE_URL.to_csv(need_url_file,index_label=False, index=False)
def getundo(alldf,fsuccess,prname=r'C:\Users\ranyu\Desktop\download_work\china.path.raw.csv'):
    todofile = os.path.join(os.path.split(fsuccess)[0], 'todo_add.txt')
    prlist = pd.read_csv(prname)
    succeed_pd = pd.read_csv(fsuccess)
    todolist = pd.DataFrame()
    for i in range(len(prlist)):
        thispr = alldf.loc[(alldf['WRS_PATH'] == prlist['PATH'][i]) & (ldst_df['WRS_ROW'] == prlist['ROW'][i])]
        thispr = thispr.loc[(thispr.CLOUD_COVER <= 50)]
        todolist = todolist.append(thispr)
    todo_add = todolist.sort_values(by=['COLLECTION_CATEGORY'])
    todo_add=todo_add.loc[todo_add.duplicated(['SCENE_ID'])==False]
    todo_add = todo_add.loc[~todo_add['PRODUCT_ID'].isin(succeed_pd['PRODUCT_ID'])]
    todo_add = todo_add.loc[todo_add.SPACECRAFT_ID == 'LANDSAT_5']
    #去重，优先C1
    # todo_add = todo_add.loc[todo_add.COLLECTION_NUMBER != 'PRE']
    todo_add.BASE_URL.to_csv(todofile, index_label=False, index=False)

if __name__=='__main__':
    filename=r"D:\download_work\index.csv"
    # need_url_file=r'D:\download_work\0404_need.csv'
    ldst_df = pd.read_csv(filename, usecols=['SCENE_ID', 'SENSOR_ID', 'PRODUCT_ID','SPACECRAFT_ID', 'DATE_ACQUIRED', 'COLLECTION_NUMBER',
                                             'COLLECTION_CATEGORY', 'WRS_PATH', 'WRS_ROW', 'CLOUD_COVER', 'BASE_URL','NORTH_LAT'],
                          dtype={'COLLECTION_NUMBER': str}, parse_dates=['DATE_ACQUIRED'])
    errorfilename=r'D:\google_ranyu\ranyu\RS_Machine\download_work\error_list.txt'

    # error_df=pd.read_csv(errorfilename)
    # interact=error_df.join(ldst_df,on='PRODUCT_ID',how ='inner')
    # interact.BASE_URL.to_csv(need_url_file,index_label=False, index=False)

    prlist = pd.read_csv(r'C:\Users\ranyu\Desktop\download_work\china.path.raw.csv')
    filelistfold = r'C:\Users\ranyu\Desktop\download_work\prlist'
    todofile = r'C:\Users\ranyu\Desktop\download_work\tododown[0,50].txt'
    todolist = pd.DataFrame()
    for i in range(len(prlist)):
        thispr = ldst_df.loc[(ldst_df['WRS_PATH'] == prlist['PATH'][i]) & (ldst_df['WRS_ROW'] == prlist['ROW'][i])]
        thispr = thispr.loc[(thispr.CLOUD_COVER <= 50)]
        # listname2r = os.path.join(filelistfold,
        #                           str(prlist['PATH'][i]).zfill(3) + str(prlist['ROW'][i]).zfill(3) + '.txt')
        # if os.path.exists(listname2r):
        #     scenelist = open(listname2r).readlines()
        #     scenelist = [os.path.splitext(os.path.splitext(os.path.basename(line))[0])[0] for line in scenelist]
        #     todolist = todolist.append(thispr.loc[~thispr.SCENE_ID.isin(scenelist)])
        # else:
        todolist = todolist.append(thispr)
    succeed_file=r'E:\GoogleDrive\ranyu\RS_Machine\download_work\success_list.txt'
    succeed_pd=pd.read_csv(succeed_file)
    todo_add = todolist.sort_values(by=['COLLECTION_CATEGORY'])
    todo_add=todo_add.loc[todo_add.duplicated(['SCENE_ID'])==False]
    todo_add = todo_add.loc[~todo_add['PRODUCT_ID'].isin(succeed_pd['PRODUCT_ID'])]
    todo_add = todo_add.loc[todo_add.SPACECRAFT_ID == 'LANDSAT_5']
    #去重，优先C1
    # todo_add = todo_add.loc[todo_add.COLLECTION_NUMBER != 'PRE']
    todo_add.BASE_URL.to_csv(todofile, index_label=False, index=False)


