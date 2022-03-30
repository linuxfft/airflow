import json
import os
from typing import Optional

import pendulum
from sqlalchemy import create_engine

from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.entities.samba import SmbFile

from plugins.utils.logger import generate_logger
import datetime
import datetime as dt
from datetime import timedelta


RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

DAG_ID = 'samba_results_dag'
TASK_ID = 'samba_results_task'


File_NAME = str(datetime.datetime.now().strftime('%Y-%m-%d %H.%M'))+'.csv'

AIRFLOW__CORE__SQL_ALCHEMY_CONN = 'postgresql+psycopg2://postgres:airflow@postgres/airflow'
IS_DEBUG = RUNTIME_ENV != 'prod'

_logger = generate_logger(__name__)

engine = create_engine(AIRFLOW__CORE__SQL_ALCHEMY_CONN, isolation_level="SERIALIZABLE")


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
    'on_failure_callback': onSambaFail,
    'on_success_callback': onSambaSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}

samba: Optional[SmbFile] = None

def save_results():
    global samba
    if not samba:
        login, password, host, port, extra = SmbFile.get_samba_args(key='qcos_samba')
    try:
        data = json.loads(extra)
        value = list(data.values())
        smb_folder = value[0]
        my_name = value[1]
        remote_name = value[2]
        samba = SmbFile(login, password, host, port, my_name, remote_name)
    except Exception as e:
        raise Exception("连接参数配置错误,正确格式为:"
                        "{\"smb-folder\": \"共享文件名\", \"my-name\": \"此设备名\", \"remote-name\": \"目标名\"}")
    try:
        samba.Smb_connect()
        _logger.info('已连接')
       # samba.createDir(File_NAME, SMB_FOLDER)
        samba.upload(File_NAME, smb_folder)
    except Exception as e:
        raise Exception("保存文件失败")
    finally:
        samba.Smb_disconnect()
        _logger.info('已断开连接')



dag = DAG(
    dag_id=DAG_ID,
    default_args=desoutter_default_args,
    schedule_interval=timedelta(hours=4),  # 执行周期，表示每小时执行四次
    description=u'定期保存拧紧结果至共享文件夹',
    catchup=False,
    concurrency=1,
    max_active_runs=1,
    tags=['samba']
)

samba_results_task = PythonOperator(
    task_id=TASK_ID,  # task_id
    python_callable=save_results,  # 指定要执行的函数
    dag=dag,  # 指定归属的dag
    retries=2,  # 重写失败重试次数，如果不写，则默认使用dag类中指定的default_args中的设置
)

