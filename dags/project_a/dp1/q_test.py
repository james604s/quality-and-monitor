import os
import sys
import requests
import json
import zipfile
import glob
import re
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

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
# sys.path.extend([os.getcwd(),os.getcwd()+"/crawler_scripts"])

def _check_mongo_conn(**kwargs):
    """
    desc:
    :param kwargs:
    :return:
    """
    m_client = MongoClient("mongodb://mongodb:mongodb123@mongodb:27017/")
    db_list = m_client.list_database_names()
    if "travel-jump" in db_list:
        print("DB: travel-jump 已存在!")
        db = m_client['travel-jump']
        db_collection_list = db.list_collection_names()
        if "activities_list" and "activities_rating_info" in db_collection_list:
            print("travel-jump: activities_list&activities_rating_info 已存在!")
            m_client.close()
            return 1
    m_client.close()
    return 0

def _check_source_conn(**kwargs):
    import requests
    url = kwargs.get("url", None)
    headers = kwargs.get("headers", None)
    res = requests.get(url, headers=headers)
    res.raise_for_status()

def _create_table_if_not_exists(**kwargs):
    """
    desc:
    :param kwargs:
    :return:
    """
    check_mongo_conn_status = kwargs['ti'].xcom_pull(task_ids='check_mongo_conn')
    if check_mongo_conn_status == 0:
        m_client = MongoClient("mongodb://mongodb:mongodb123@mongodb:27017/")
        db = m_client['travel-jump']
        db_col = db["activities_list"]
        db_col.insert_one({"create":"create"})
        db_col.delete_one({"create":"create"})
        print("db_collection: activities_list create succussful!")

        db_col = db["activities_rating_info"]
        db_col.insert_one({"create":"create"})
        db_col.delete_one({"create":"create"})
        print("db_collection: activities_rating_info create succussful!")
        m_client.close()

dt_today = datetime.strftime(datetime.today().date(), "%Y-%m-%d")

default_args = {
    "owner":"airflow",
    # "email": ["james604s@gmail.com"],
    # "email_on_failure": True,
    # "retries": 1,
    # "retry_delay": 60,
}

dag = DAG('klook_activities',
          start_date=datetime(2022,3,1),
          schedule_interval="*/1 * * * *",
          tags=["klook", "crawler", "activities"],
          default_args=default_args,
          catchup=False)

begin = DummyOperator(
    task_id="begin"
)

end = DummyOperator(
    task_id="end"
)

uci_check_http = HttpSensor()

uci_dwd_data = PythonOperator()

uci_unzip = PythonOperator()

uci_transfor = PythonOperator()

uci_uci_ge_validate = GreatExpectationOperator()

uci_data_to_sql = PythonOperator()

uci_check_sql = SqlSensor()

uci_create_table_if_not_exists = PostgreSQLOperator


"""
check_http >> dwd_data >> unzip >> transfor >> ge >> to_sql 
check_sql >> create_table_if_not_exists

"""
