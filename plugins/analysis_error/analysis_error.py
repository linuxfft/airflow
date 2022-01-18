from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base_hook import BaseHook
import os
from aiohttp_retry import RetryClient
from aiohttp import ClientTimeout
import json
from http import HTTPStatus
import requests

class AnalysisErrorHook(BaseHook):

    @classmethod
    def listen_to_analysis_errors(cls):
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


class AnalysisErrorPlugin(AirflowPlugin):
    name = "analysis_error_plugin"

    hooks = [AnalysisErrorHook]
