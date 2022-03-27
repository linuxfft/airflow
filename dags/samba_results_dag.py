import os
from typing import Optional

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.entities.samba import SmbFile

from plugins.utils.logger import generate_logger
import logging
import datetime as dt
from datetime import timedelta

from plugins.utils.utils import get_curve

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

DAG_ID = 'samba_results_dag'
TASK_ID = 'samba_results_task'

File_NAME =
SMB_PATH =
LOCAL_PATH =
MY_NAME =
REMOTE_NAME =


IS_DEBUG = RUNTIME_ENV != 'prod'

_logger = generate_logger(__name__)

File_NAME =
SMB_PATH =
LOCAL_PATH =

def onSambaFail(context):
    _logger.debug("{0} Run Fail".format(context))

def onSambaSuccess(context):
    _logger.info("{0} Run Success".format(context))

local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'qcos',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': onSambaFail(),
    'on_success_callback': onSambaSuccess(),
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}

samba : Optional[SmbFile] = None

def save_results(result):
    global samba
    if not samba:
        h, p, lo, ps = SmbFile.get_samba_args(key='qcos_samba')
        samba = SmbFile(h, p, lo, ps, MY_NAME, REMOTE_NAME)
    try:
        samba.Smb_connect()
        samba.createDir('result', SMB_PATH)
        samba.upload(File_NAME, SMB_PATH, LOCAL_PATH)
    except:




dag = DAG(
    dag_id=DAG_ID,
    default_args=desoutter_default_args,
    schedule_interval=timedelta(hours=4),  # 执行周期，表示每小时执行四次
    description=u'定期保存拧紧结果至共享文件夹'
)

samba_results_task = PythonOperator(
    task_id=TASK_ID,  # task_id
    python_callable=save_results,  # 指定要执行的函数
    dag=dag,  # 指定归属的dag
    retries=2,  # 重写失败重试次数，如果不写，则默认使用dag类中指定的default_args中的设置
)

