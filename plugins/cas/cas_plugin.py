from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base_hook import BaseHook
import os
from aiohttp_retry import RetryClient
from aiohttp import ClientTimeout
import json
from http import HTTPStatus
import requests


class CasHook(BaseHook):
    """
    与CAS交互
    """

    def __init__(self, role='all'):
        super(CasHook, self).__init__()
        if role == 'analysis':
            conn_id = 'cas_analysis'
        elif role == 'training':
            conn_id = 'cas_training'
        else:
            conn_id = 'cas_server'

        self.conn_id = conn_id

        try:
            from airflow.models.connection import Connection
            self.connection = Connection.get_connection_from_secrets(conn_id)
        except Exception as e:
            self.log.error(e)
            self.connection = None
        if self.connection:
            self.extras = self.connection.extra_dejson.copy()
            self.uri = '{scheme}://{host}{port}'.format(
                scheme='http',
                host=self.connection.host,
                port='' if self.connection.port is None else ':{}'.format(self.connection.port)
            )
        else:
            raise Exception(f'没有配置cas连接{conn_id}')

    @property
    def trigger_analyze_endpoint(self):
        """
        触发cas分析的url
        @return: str
        """
        return '{}/cas/analysis'.format(self.uri)

    @property
    def trigger_training_endpoint(self):
        """
        触发cas训练的url
        @return: str
        """
        return "{}/cas/invalid-curve".format(self.uri)

    async def trigger_analyze(self, params, timeout=ClientTimeout(total=30), retry_attempts=5):
        """
        触发分析，同步获取分析结果
        @param params: 用于触发分析的数据
        @param timeout: 请求超时时间
        @param retry_attempts: 请求重试次数
        """
        headers = {
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        data = {
            'conf': params
        }
        try:
            url = self.trigger_analyze_endpoint
            if not params:
                raise Exception(u'数据为空')
            self.log.info('参数验证通过，触发分析...')
            async with RetryClient(timeout=timeout) as client:
                async with client.post(headers=headers, url=url, retry_attempts=retry_attempts, json=data) as r:
                    r.raise_for_status()
                    resp = await r.read()
                    self.log.debug("trigger analyze: {}, resp: {}".format(json.dumps(data), resp))
                    return resp
        except BaseException as e:
            self.log.error(
                "push_result_to_training_server except: {}".format(repr(e)))
            raise e

    def trigger_training(self, data):
        """
        触发异步训练
        @param data: 用于触发训练的数据
        """
        json_data = {
            'conf': data
        }
        try:
            self.log.info('posting to training server')
            self.log.debug('data:{}'.format(json.dumps(json_data, indent=4)))
            resp = requests.post(headers={'Content-Type': 'application/json'}, url=self.trigger_training_endpoint,
                                 json=json_data,
                                 timeout=(3.05, 27))
            self.log.info('training server response')
            if resp.status_code != HTTPStatus.OK:
                raise Exception(f'training server returns an error: {resp.content}')
        except Exception as e:
            self.log.error(repr(e))
            raise Exception(str(e))


class CasPlugin(AirflowPlugin):
    name = "cas_plugin"

    hooks = [CasHook]

    @classmethod
    def on_load(cls, *args, **kwargs):
        pass
