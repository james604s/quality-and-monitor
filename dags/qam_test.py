import os
import sys
import pandas as pd

from pymongo import MongoClient
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
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
