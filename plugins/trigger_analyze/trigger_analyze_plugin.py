import os
import asyncio
from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from plugins.utils.utils import get_curve_params, get_curve, get_result
from typing import Dict
from airflow.api.common.experimental import trigger_dag as trigger
from distutils.util import strtobool
from flask import jsonify, request, Blueprint
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.app import csrf


_logger = LoggingMixin().log

try:
    ENV_ALWAYS_TRIGGER_ANAY = strtobool(
        os.environ.get('ENV_ALWAYS_TRIGGER_ANAY', 'true'))
except:
    ENV_ALWAYS_TRIGGER_ANAY = True


class TriggerAnalyzeHook(BaseHook, ABC):

    @staticmethod
    def trigger_push_result_dag(params):
        _logger.info('pushing result to mq...')
        from plugins.publish_result.publish_result_plugin import PublishResultHook
        PublishResultHook.trigger_publish('tightening_result', params)

    @staticmethod
    def is_rework_result(result) -> bool:
        if not result:
            return False
        job = result.get('job', 0)
        batch_count = result.get('batch_count', 0)
        if (not job) and (not batch_count):
            return True
        return False

    @staticmethod
    def get_result_type(result: Dict) -> str:
        ret = 'normal'
        if TriggerAnalyzeHook.is_rework_result(result):
            return 'rework'
        return ret

    @staticmethod
    def should_skip_analysis(params: Dict) -> bool:
        should_analyze = params.get('should_analyze', True)
        if not should_analyze:
            _logger.info('接收到不分析指令，跳过分析...')
            return True
        if ENV_ALWAYS_TRIGGER_ANAY:
            return False
        if TriggerAnalyzeHook.is_rework_result(params.get('result')):
            _logger.info('返修工位曲线，跳过分析...')
            return True
        return False

    @staticmethod
    def prepare_trigger_params(params):
        result_body = params.get('result', None)
        entity_id = params.get('entity_id', None)
        bolt_number = params.get('bolt_number', None)
        craft_type = params.get('craft_type', None)
        assert result_body is not None, '触发分析的数据中拧紧结果为空'
        assert entity_id is not None, '触发分析的数据中entity_id为空'
        assert bolt_number is not None, '触发分析的数据中bolt_number为空'
        assert craft_type is not None, '触发分析的数据中craft_type为空'
        new_param = params.copy()
        # 螺栓编码生成规则：控制器名称-job号-批次号
        result_type = TriggerAnalyzeHook.get_result_type(params.get('result'))
        _logger.info("type: {}, entity_id: {} bolt_number: {}"
                     .format(result_type, entity_id, bolt_number))

        try:
            curve_params = get_curve_params(bolt_number)
        except Exception as e:
            _logger.error(e)
            curve_params = {}
            _logger.error('无法获取曲线参数（{}/{}）'.format(bolt_number, craft_type))

        new_param.update(curve_params)
        return new_param

    @staticmethod
    def do_trigger_analyze(params, task_instance):
        should_skip_analysis = TriggerAnalyzeHook.should_skip_analysis(params)
        if should_skip_analysis:
            return
        from plugins.result_storage.result_storage_plugin import ResultStorageHook
        entity_id = params.get('entity_id', None)
        ResultStorageHook.bind_analyze_task(
            entity_id,
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.execution_date
        )
        new_param = TriggerAnalyzeHook.prepare_trigger_params(params)
        TriggerAnalyzeHook.trigger_push_result_dag(new_param)
        from plugins.cas.cas_plugin import CasHook
        cas = CasHook(role='analysis')
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(cas.trigger_analyze(new_param))
        loop.close()
        return result

    @staticmethod
    def trigger_analyze(params):
        # 此处未来或将不创建分析任务。
        # 添加此方法意在统一外部接口，不在不同地方调用trigger_dag，便于维护
        trigger.trigger_dag('curve_analyze_dag', conf=params, replace_microseconds=False)

    @staticmethod
    def trigger_with_entity_id(entity_id):

        result = get_result(entity_id)
        curve = get_curve(entity_id)
        conf = TriggerAnalyzeHook.get_trigger_param_from_result(entity_id, result, curve)
        trigger.trigger_dag('curve_analyze_dag', conf=conf, replace_microseconds=False)

    @staticmethod
    def get_trigger_param_from_result(entity_id, result, curve):
        conf = {}
        for k in ['update_time', 'execution_date', 'training_execution_date']:
            if k in result.keys():
                result.pop(k)
        conf.update({
            'bolt_number': result.get('bolt_number'),
            'entity_id': entity_id,
            'should_analyze': True,
            'craft_type': result.get('craft_type'),
            'result': result,
            'curve': curve
        })
        return conf


class TriggerAnalyzeOperator(BaseOperator):

    def execute(self, context):
        params = context['dag_run'].conf
        task_instance = context['task_instance']
        TriggerAnalyzeHook.do_trigger_analyze(params, task_instance)



bp = Blueprint('trigger_analyze', __name__)


@bp.route('/analysis_result', methods=['PUT'])
def put_analyze_result():
    try:
        data = request.get_json(force=True)
        entity_id = data.get('entity_id', None)
        measure_result = data.get('measure_result', None)
        curve_mode = data.get('result', None)
        verify_error = data.get('verify_error', None)
        from plugins.result_storage.result_storage_plugin import ResultStorageHook
        ResultStorageHook.save_analyze_result(
            entity_id,
            measure_result,
            curve_mode,
            verify_error
        )
        resp = jsonify({'response': 'ok'})
        resp.status_code = 200
        return resp
    except Exception as e:
        resp = jsonify({'error': repr(e)})
        resp.status_code = 500
        return resp


# Defining the plugin class
class TriggerAnalyzePlugin(AirflowPlugin):
    name = "trigger_analyze_plugin"
    hooks = [TriggerAnalyzeHook]
    flask_blueprints = [csrf.exempt(bp)]
