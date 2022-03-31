import os
import requests
import json
import zipfile
import glob
import re
import pandas as pd
from datetime import datetime

# https://archive.ics.uci.edu/ml/machine-learning-databases/00357/occupancy_data.zip
def _download_file(**kwargs):
    resource_file_link = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00357/occupancy_data.zip'
    result = requests.get(resource_file_link, stream=True)
    print(f"DOWNLOAD_LINK = {resource_file_link}")

    print("CONTENT_TYPE = ")
    print(result.headers.get('content-type'))
    print("apparent_encoding = ")
    print(result.headers.get('encoding'))
    print("header = ")
    print(result.headers)

    fname = datetime.strftime(datetime.now(),'%Y%m%d%H') + "_occupancy_data.zip"
    # Download ZIP file
    with open(os.getcwd() + f"/data/{fname}", "wb") as f:
        for chunk in result.iter_content(chunk_size=128):
            f.write(chunk)

def _unzip_file(**kwargs):
    fname = datetime.strftime(datetime.now(), '%Y%m%d%H') + "_occupancy_data.zip"
    fname_nozip = fname.replace('.zip', '')
    zip_file = os.getcwd() + f"/data/{fname}"
    print("ZIP_FILE_PATH = ")
    print(zip_file)

    unzip_file_path = os.getcwd() + f"/data/{fname_nozip}"
    print("UNZIP_FILE_PATH = ")
    print(unzip_file_path)

    from pathlib import Path
    unzip_dir = Path(unzip_file_path)
    # create unzip destination folder
    if not unzip_dir.exists():
        unzip_dir.mkdir()

    with zipfile.ZipFile(zip_file) as z:
        for i in z.namelist():
            unzip_location = i.encode('cp437').decode('big5')
            n = Path(os.getcwd()+'/data/'+unzip_location)

            # make sure file unzip in unzip folder
            if fname_nozip + '/' not in unzip_location:
                n = Path(os.getcwd() + '/data/' + fname_nozip + '/' + unzip_location)

            print("LOCATION = ")
            print(n.absolute())
            if i[-1] == '/':
                print("IS DIR CREATE DIR IF NOT EXIST")
                if not n.exists():
                    n.mkdir()
            else:
                with n.open('wb') as w:
                    print("IS FILE WRITE FILE ")
                    w.write(z.read(i))

    # 備註：zipfile unzip 會有中文亂碼問題
    # with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    #     zip_ref.extractall(unzip_file_path)
    return {'fp': unzip_file_path}

def pd_read_txt(**kwargs):
    name_list = ['datatest','datatest2','datatraining']
    for i in name_list:
        with open(f"dwd_zip/{i}.txt","r",encoding="utf8") as f:
            lines = f.readlines()
        lines.pop(0)
        pre_data = map(lambda x: re.sub('[\"|\n]',"",x).split(","), lines)
        cols = ["id", "date", "temperature", "humidity", "light", "co2", "humidityratio","Occupancy"]
        locals()['df_'+str(i)] = pd.DataFrame(pre_data,columns=cols)
    print(df_datatest)
    print(df_datatest2)
    print(df_datatraining)
"""
import pandas as pd
df = pd.read_table('dwd_zip/datatest.txt',sep="\t")
"""
with open("dwd_zip/datatest.txt","r",encoding="utf8") as f:
    lines = f.readlines()
lines.pop(0)
# pre_data = map(lambda x: x.replace('"',"").replace('\n',"").split(","), lines)
pre_data = map(lambda x: re.sub('[\"|\n]',"",x).split(","), lines)
cols = ["id", "date", "temperature", "humidity", "light", "co2", "humidityratio","Occupancy"]
df = pd.DataFrame(pre_data,columns=cols)

