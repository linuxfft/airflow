# -*- coding:utf-8 -*-
import datetime as dt
import json
import os
import pprint
from datetime import timedelta
from typing import Dict

import pika
from plugins.factory_code.factory_code import get_factory_code

from airflow.models import DAG, Variable
import pendulum
from airflow.operators.python_operator import PythonOperator
from plugins.entities.result_mq import ClsResultMQ
from plugins.utils.logger import generate_logger


def analysis_error_listener():
    pass


analysis_error_listener_concurrency = int(os.getenv('ANALYSIS_ERROR_LISTENER_CONCURRENCY', '16'))

listener_dag = DAG(
    dag_id='analysis_error_listener',
    description=u'监听分析异常',
    schedule_interval=timedelta(milliseconds=500),
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': [],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    concurrency=analysis_error_listener_concurrency,
    max_active_runs=analysis_error_listener_concurrency
)

listener_task = PythonOperator(
    provide_context=True,
    task_id='mq_result_storage_task',
    dag=listener_dag,
    priority_weight=9,
    python_callable=analysis_error_listener
)

analysis_error_handler_concurrency = int(os.getenv('ANALYSIS_ERROR_HANDLER_CONCURRENCY', '16'))

handler = DAG(
    dag_id='analysis_error_handler',
    description=u'处理分析异常',
    schedule_interval=None,
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': [],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    concurrency=analysis_error_handler_concurrency,
    max_active_runs=analysis_error_handler_concurrency
)
