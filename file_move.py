#coding:utf-8
import os,sys
import shutil

# work
# F:\Landsat\L5\114\027\LT05_L1TP_114027_19840801_20170220_01_T1
# 如上行例子 比对\114\027与后面的影像ID中的114027是否匹配
# 若不匹配 则根据影像ID移动到对应文件夹

if __name__=='__main__':
    root_dir = sys.argv[1]

    for dir_name, subdir_list, file_list in os.walk(root_dir):
        # 获得文件夹中的row col
        file_row_col = dir_name.split('\\')
        file_row = file_row_col[-2]
        file_col = file_row_col[-1]
        for scene_name in subdir_list:
            if len(scene_name)<10:
                continue
            # 获得文件名中的row col
            rowcol = os.path.basename(scene_name).split('_')[2]
            orbit_path = rowcol[0:3]
            orbit_row = rowcol[3:6]
            if (orbit_path == file_row) & (orbit_row == file_col):
                # 如果匹配则不动
                pass
            else:
                # 不匹配的话输出错误目录
                print(os.path.join(dir_name, scene_name))
                # 获得正确目录
                right_path = os.path.join(root_dir,orbit_path,orbit_row)#dir_name.replace(file_row, row).replace(file_col, col)
                if(os.path.exists(right_path)):
                    # 正确目录存在直接移动
                    shutil.move(os.path.join(dir_name, scene_name), right_path)
                else:
                    # 正确目录不存在先创建再移动
                    os.mkdir(right_path)
                    shutil.move(os.path.join(dir_name, scene_name), right_path)


