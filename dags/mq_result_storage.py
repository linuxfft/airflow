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

_logger = generate_logger(__name__)


def on_fail(context):
    _logger.debug("{0} Run Fail".format(context))


def on_success(context):
    _logger.info("{0} Run Success".format(context))


def result_handler(channel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body: bytes):
    '''
    :param channel: BlockingChannel
    :param method: spec.Basic.Deliver
    :param properties: spec.BasicProperties
    :param body: bytes
    '''
    try:
        if not body:
            return
        channel = method.routing_key or ''
        if not body or not channel:
            return
        data = body
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        data_dict: Dict = json.loads(data)
        _logger.debug("Receive Analysis Result, data: {}".format(pprint.pformat(data, indent=4)))
        entity_id = data_dict.get('entity_id', None)
        if entity_id is None:
            _logger.debug("entity_id not in result")
            return
        # data = {
        #     'result': curve_mode,
        #     'verify_error': verify_error,
        #     'entity_id': trigger.entity_id,
        #     'measure_result': trigger.measure_result,
        #     'result_data': trigger.result,
        #     'curve_data': trigger.curve
        # }
        result_data = data_dict.get('result_data', None)
        curve_data = data_dict.get('curve_data', None)
        measure_result = data_dict.get('measure_result', None)
        curve_mode = data_dict.get('result', None)
        verify_error = data_dict.get('verify_error', None)
        factory_code = data_dict.get('factory_code', None)

        from plugins.result_storage.result_storage_plugin import ResultStorageHook

        ResultStorageHook.save_result(
            entity_id,
            result_data,
            **ResultStorageHook.generate_extra_data(result_data, True, factory_code)
        )
        ResultStorageHook.save_curve(
            entity_id,
            curve_data
        )
        ResultStorageHook.save_analyze_result(
            entity_id, measure_result, curve_mode, verify_error
        )

        _logger.info(f"{entity_id} all saved")
    except Exception as e:
        _logger.error("trigger_handler error: {}".format(repr(e)))


def watch_mq_results(*args, **kwargs):
    mq_connection = ClsResultMQ(**ClsResultMQ.get_result_mq_args(key='qcos_rabbitmq'))
    factory_code = get_factory_code()
    default_queue_config = {
        'queue': f'qcos_result_storage',
        'exchange': f'qcos_analysis_result',
        'exchange_type': 'fanout',
        'routing_key': f'*',
        'durable': True
    }
    queue_config = Variable.get("result_storage_queue_config", default_var=default_queue_config,
                                deserialize_json=True)
    default_queue_config.update(queue_config)
    mq_connection.doSubscribe(message_handler=result_handler, passive=False, **default_queue_config)
    mq_connection.run(queue=default_queue_config.get('queue'))
    mq_connection.doUnsubscribe(default_queue_config.get('queue'))


dag = DAG(
    dag_id='mq_result_storage',
    description=u'从mq获取结果数据并保存',
    schedule_interval=timedelta(milliseconds=500),
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': ['support@desoutter.cn'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    concurrency=1,
    max_active_runs=1
)

task = PythonOperator(
    provide_context=True,
    task_id='mq_result_storage_task',
    dag=dag,
    priority_weight=9,
    python_callable=watch_mq_results
)
