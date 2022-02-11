# -*- coding:utf-8 -*-

import os
import json
import uuid
import pprint
import datetime as dt
from datetime import timedelta
import pendulum
import logging
import pika
from typing import Optional
from airflow.models import DAG
from qcos_addons.models.curve_template import CurveTemplateModel
from typing import Dict
from airflow.operators.python_operator import PythonOperator
from plugins.entities.redis import ClsRedisConnection, gen_template_key
from plugins.entities.result_mq import ClsResultMQ
from plugins.utils.utils import parse_template_name
from plugins.rabbitmq.rabbimq_plugin import RabbitmqOperator

CURVE_TEMPLATE_UPGRADE_TASK = 'curve_template_upgrade'

CURVE_TEMPLATE_KEY_PREFIX = os.environ.get(
    "CURVE_TEMPLATE_KEY_PREFIX", "qcos_templates")

DAG_ID = 'curve_template_upgrade'

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

if RUNTIME_ENV == 'prod':
    schedule_interval = '@once'
    loggingLevel = logging.INFO
else:
    schedule_interval = '@once'
    loggingLevel = logging.DEBUG

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())

_logger.setLevel(loggingLevel)


def onUpgradeCurveTmplFail(context):
    _logger.debug("{0} Run Fail".format(context))


def onUpgradeCurveTmplSuccess(context):
    _logger.debug("{0} Run Success".format(context))


local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'qcos',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': onUpgradeCurveTmplFail,
    'on_success_callback': onUpgradeCurveTmplSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}

redis_connection: Optional[ClsRedisConnection] = None


def template_upgrade_handler(ch, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body: bytes):
    try:
        if not body:
            return
        data = body
        channel = method.routing_key or ''
        if not data or not channel:
            return
        if isinstance(channel, bytes):
            channel = channel.decode('utf-8')
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        template: Dict = json.loads(data)
        _logger.debug("Recv Template Data, key:{}, data: {}".format(channel, pprint.pformat(template, indent=4)))
        template_name = parse_template_name(channel)
    except Exception as e:
        _logger.error("template_upgrade_handler error: {}".format(repr(e)))
        raise e
    try:
        key, val = CurveTemplateModel.get_fuzzy_active(key=template_name, deserialize_json=True)
        _logger.debug("Get Template Var: {}".format(key))
        windows = val.get('curve_param').get('windows', None) if val.get('curve_param', False) else None
        if windows:
            template.update({'windows': windows})
        template.update({'version': val.get('version', 0) + 1})
        CurveTemplateModel.set(key=key, value=template, serialize_json=True)  # 此业务场景下 params不会变化故覆盖现有的variable

    except KeyError as e:
        _logger.info(f"收到不存在的模板{template_name}，不进行任何操作")
        return
        # 没有这个key 重新创建这个key
        # key = "{}@@{}".format(template_name, str(uuid.uuid4()))
        # CurveTemplateModel.set(key=key, value=template, serialize_json=True)
    except Exception as e:
        _logger.error("template_upgrade_handler error: {}".format(repr(e)))
    try:
        global redis_connection
        if redis_connection is None:
            redis_connection = ClsRedisConnection()
        redis_connection.store_templates({
            template_name: json.dumps(template)
        })
    except Exception as e:
        _logger.error('store curve template to redis error: {}'.format(repr(e)))


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线分析-曲线模板更新',
    start_date=dt.datetime(2020, 1, 1, tzinfo=local_tz),
    concurrency=1,
    max_active_runs=1,
    schedule_interval=timedelta(milliseconds=500),
    catchup=True,
    default_args= {
        'owner': 'qcos',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    tags=['training', 'mq']
)

upgrade_curve_template_task = RabbitmqOperator(
    task_id=CURVE_TEMPLATE_UPGRADE_TASK,
    dag=dag,
    priority_weight=9,
    mq_config={
        'conn_id': 'qcos_rabbitmq',
        'queue': os.environ.get('MQ_TEMPLATE_QUEUE', 'qcos_templates_airflow'),
        'queue_args': {
            'durable': True
        },
        'exchange': os.environ.get('MQ_TEMPLATE_EXCHANGE', 'qcos_templates'),
        'exchange_args': {
            'exchange_type': 'fanout'
        },
        'binding_args': {
            'routing_key': gen_template_key('*'),
        },
        'message_handler': template_upgrade_handler
    }
)
