#!/usr/bin/python3
#REMEMBER change the targetDir PATH(../analysisResults/XXXXX) which you want to concatenate!!
import os

# 先取得該檔案夾內所有的檔案名稱
def get_list(targetDir):
    all_name = os.listdir(targetDir)
    return all_name

if __name__ == '__main__':
    targetDir = "../analysisResults/staying_time_analysis"
    # print(get_list(targetDir))
    # print(len(get_list(targetDir)))
    count=0
    for g in get_list(targetDir):
        if g != '_SUCCESS':
            with open(targetDir + '/' + g, "r", encoding='utf-8') as f:
                onion=f.read()
            with open("catResult/" + "onionresult4" + targetDir.split("/")[-1] + ".json", "a") as f:
                f.write(onion)
            count+=1
    print(count)

