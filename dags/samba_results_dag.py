from typing import Optional
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.entities.samba import SmbFile
from plugins.utils.logger import generate_logger
import datetime as dt
from datetime import timedelta


DAG_ID = 'samba_results_dag'
TASK_ID = 'samba_results_task'


_logger = generate_logger(__name__)


def push_data(**context):
    context['task_instance'].xcom_push(key='test_key', value='test_val')

def pull_data(**context):
    test_data = context['ti'].xcom_pull(key='test_key')

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


def save_results(**context):
    global samba
    data = SmbFile.get_samba_args(key='qcos_samba')
    try:
        smb_folder = data.get('smb-folder', None)
        if not samba:
            samba = SmbFile(
                data.get('username', None),
                data.get('password', None),
                data.get('host', None),
                data.get('port', None),
                data.get('my-name', None),
                data.get('remote-name', None)
            )
    except Exception:
        raise Exception("连接参数配置错误,正确格式为:"
                        "{\"smb-folder\": \"共享文件名\", \"my-name\": \"此设备名\", \"remote-name\": \"目标名\"}, \"delta\": \"间隔小时数\"}")
    try:
        samba.Smb_connect()
        _logger.info('smb已连接')
        result_pk = context['task_instance'].xcom_pull(key="pk", include_prior_dates=True, task_ids=TASK_ID, dag_id=DAG_ID)
        pk = samba.uploadDir(smb_folder, result_pk)
        _logger.info('数据已经保存')
        if pk:
            context['task_instance'].xcom_push(key="pk", value=pk)
    except Exception as e:
        _logger.error('已断开连接')
        raise e
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
    provide_context=True
)

