import os,sys

if __name__=='__main__':
    root_dir = sys.argv[1]
    error_txt = 'error_list.txt'
    success_txt = 'success_list.txt'
    error_file = open(os.path.join(root_dir, error_txt), 'w')
    success_file = open(os.path.join(root_dir, success_txt), 'w')
    error_file.write('PRODUCT_ID\n')
    success_file.write('PRODUCT_ID\n')
    # root_dir = os.path.join(root_dir, 'error')

    for dir_name, subdir_list, file_list in os.walk(root_dir):
        break_flag = False
        if len(subdir_list) < 1:

            for file_name in file_list:
                if os.path.basename(file_name).split('.')[-1] == 'gstmp':
                    print(os.path.basename(dir_name) + ' is .gstmp')
                    error_file.write(os.path.split(dir_name)[-1] + '\n')
                    tmpname=os.path.join(dir_name,file_name)
                    os.remove(tmpname)
                    print('del'+tmpname)
                    break_flag = True
                    # break
            if break_flag:
                continue

            if len(file_list) == 0:
                print(dir_name + ' does not have any scene')
                error_file.write(os.path.split(dir_name)[-1] + '\n')
                continue
            if len(file_list) != 14 and os.path.split(dir_name)[-1][5:9]!='L1GS' and len(os.path.split(dir_name)[-1])!=21:
                print(dir_name + ' file number is not right')
                error_file.write(os.path.split(dir_name)[-1] + '\n')
                continue
            if len(file_list) != 8 and len(os.path.split(dir_name)[-1]) == 21:
                print(dir_name + ' file number is not right')
                error_file.write(os.path.split(dir_name)[-1] + '\n')
                continue
            if len(file_list) != 11 and os.path.split(dir_name)[-1][5:9]=='L1GS':
                print(dir_name + ' file number is not right')
                error_file.write(os.path.split(dir_name)[-1] + '\n')
                continue
            success_file.write(os.path.split(dir_name)[-1] + '\n')
