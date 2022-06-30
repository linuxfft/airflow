# -*- coding:utf-8 -*-
import datetime as dt
import json
import os
from datetime import timedelta
from typing import Dict
import pika
from airflow.api.common.experimental import trigger_dag
from airflow.models import DAG
import pendulum
from plugins.rabbitmq.rabbimq_plugin import RabbitmqOperator
from plugins.utils.logger import generate_logger
from sqlalchemy.exc import IntegrityError
from psycopg2 import errors

_logger = generate_logger(__name__)


def on_fail(context):
    _logger.debug("{0} Run Fail".format(context))


def on_success(context):
    _logger.debug("{0} Run Success".format(context))


def result_handler(channel, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body: bytes):
    """
    处理从消息队列中接收到的结果
    @param channel: BlockingChannel
    @param method: spec.Basic.Deliver
    @param properties: spec.BasicProperties
    @param body: bytes
    """
    from plugins.result_storage.result_storage_plugin import ResultStorageHook
    from plugins.publish_result.publish_result_plugin import PublishResultHook
    if not body:
        return
    channel = method.routing_key or ''
    if not body or not channel:
        return

    # 解析结果数据
    data = body
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    data_dict: Dict = json.loads(data)
    try:
        _logger.debug(f"Receive Analysis Result, data: {data}")
        entity_id = data_dict.get('entity_id', None)
        _logger.debug(f"entity_id: {entity_id}")
        if entity_id is None:
            _logger.debug("entity_id not in result")
            return
        result_data = data_dict.get('result_data', None)
        curve_data = data_dict.get('curve_data', None)
        measure_result = data_dict.get('measure_result', None)
        factory_code = data_dict.get('factory_code', None)
        curve_mode = data_dict.get('result', None)
        verify_error = data_dict.get('verify_error', None)
    except Exception as e:
        raise Exception("解析分析结果异常: {}".format(repr(e)))

    # 保存结果
    result_exists = False
    try:
        ResultStorageHook.save_result(
            entity_id,
            result_data,
            **ResultStorageHook.generate_extra_data(result_data, True, factory_code)
        )

        PublishResultHook.trigger_publish('tightening_result', data_dict)
    # except IntegrityError as e:
    #     # 已经存在的结果不再执行后续流程，避免异常处理陷入循环
    #     if isinstance(e.orig, errors.UniqueViolation):
    #         _logger.info(f'结果{entity_id}已存在')
    #         result_exists = True
    except Exception as e:
        _logger.info(f'结果{entity_id}已存在')
        result_exists = True
    #    raise Exception("保存结果异常: {}".format(repr(e)))

    # 保存曲线
    try:
        ResultStorageHook.save_curve(
            entity_id,
            curve_data
        )
    except Exception as e:
        raise Exception("保存曲线异常: {}".format(repr(e)))

    _logger.info(f'保存控制器结果和曲线完成。')

    # 对于异常情况，触发异常处理DAG
    if curve_mode is None or verify_error is None:
        # 如果结果已经存在，则认为分析异常已经触发过（如存在分析异常），因此不再触发
        if not result_exists:
            # 分析结果不存在，视为分析异常，向消息队列中发送异常消息
            _logger.warn(f"{entity_id}分析结果异常（curve_mode：{curve_mode}，verify_error：{verify_error}）")
            _logger.info(f'正在触发异常处理任务。')
            trigger_dag.trigger_dag('analysis_failure_handler', replace_microseconds=False, conf=data_dict)
            _logger.info(f'异常处理任务触发完成。')
        return

    # 保存分析结果
    _logger.info(f'分析结果解析正常，开始保存分析结果。')
    ResultStorageHook.save_analyze_result(
        entity_id, measure_result, curve_mode, verify_error
    )
    _logger.info(f'保存分析结果完成。')
    _logger.info(f"{entity_id} all saved")


dag = DAG(
    dag_id='mq_result_storage',
    description=u'从mq获取结果数据并保存',
    start_date=dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
    catchup=False,
    schedule_interval=timedelta(seconds=1),
    default_args={
        'owner': 'qcos',
        'depends_on_past': False,
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    concurrency=1,
    max_active_runs=1,
    tags=['analyze', 'mq']
)

listener_task = RabbitmqOperator(
    task_id='mq_result_storage_task',
    dag=dag,
    priority_weight=9,
    mq_config={
        'conn_id': 'qcos_rabbitmq',
        'queue': os.environ.get('MQ_RESULT_STORAGE_QUEUE', 'qcos_result_storage'),
        'queue_args': {
            'durable': True
        },
        'exchange': os.environ.get('MQ_RESULT_STORAGE_EXCHANGE', 'qcos_result_storage'),
        'exchange_args': {
            'exchange_type': 'fanout'
        },
        'binding_args': {
            'routing_key': f'*',
        },
        'message_handler': result_handler
    }
)
