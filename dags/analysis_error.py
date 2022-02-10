# -*- coding:utf-8 -*-
import datetime as dt
import json
import logging
import os
from datetime import timedelta
from typing import Dict
from airflow.api.common.experimental import trigger_dag

import pika

from airflow.models import DAG, DagRun
import pendulum
from airflow.operators.python import PythonOperator
from plugins.rabbitmq.rabbimq_plugin import RabbitmqOperator
from plugins.utils.logger import generate_logger

_logger = generate_logger(__name__)


def analysis_error_listener(channel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties,
                            body: bytes):
    if not body:
        return
    if not body or not channel:
        return
    data = body
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    data_dict: Dict = json.loads(data)
    # 触发重试dag
    trigger_dag.trigger_dag('analysis_error_handler', replace_microseconds=False, conf=data_dict)


analysis_error_listener_concurrency = int(os.getenv('ANALYSIS_ERROR_LISTENER_CONCURRENCY', '1'))

listener_dag = DAG(
    dag_id='analysis_error_listener',
    description=u'监听分析异常',
    schedule_interval=timedelta(milliseconds=500),
    default_args={
        'owner': 'qcos',
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

listener_task = RabbitmqOperator(
    task_id='mq_analysis_error_listener_task',
    dag=listener_dag,
    priority_weight=9,
    conn_id='qcos_rabbitmq',
    queue=f'qcos_analysis_exception',
    queue_args={
        'durable': True
    },
    exchange=f'qcos_analysis_exception',
    exchange_args={
        'exchange_type': 'fanout'
    },
    binding_args={
        'routing_key': f'*',
    },
    message_handler=analysis_error_listener
)

analysis_error_handler_concurrency = int(os.getenv('ANALYSIS_ERROR_HANDLER_CONCURRENCY', '16'))

handler_dag = DAG(
    dag_id='analysis_error_handler',
    description=u'处理分析异常',
    schedule_interval=None,
    default_args={
        'owner': 'qcos',
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


def handle_analysis_error(dag_run):
    if isinstance(dag_run, DagRun):
        params = getattr(dag_run, 'conf')
    elif isinstance(dag_run, dict):
        params = dag_run.get('conf', None)
    else:
        raise Exception('无法解析触发参数')
    # 获取失败结果的entity_id
    _logger.debug(f'接收到分析异常，params:{json.dumps(params)}')
    entity_id = params.get('entity_id', None)
    # 尝试重新触发分析
    from plugins.trigger_analyze.trigger_analyze_plugin import TriggerAnalyzeHook
    TriggerAnalyzeHook.trigger_analyze_with_entity_id(entity_id)


handler_task = PythonOperator(
    provide_context=True,
    task_id='analysis_error_handler_task',
    dag=handler_dag,
    python_callable=handle_analysis_error
)
