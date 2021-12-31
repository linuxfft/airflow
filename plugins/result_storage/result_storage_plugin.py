from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from plugins.utils.utils import get_curve_args
import os
from pprint import pformat
from plugins.entities.curve_storage import ClsCurveStorage
from plugins.entities.result_storage import ClsResultStorage
from plugins.utils.utils import generate_bolt_number
from plugins.utils.utils import get_craft_type
from airflow.utils.db import provide_session
import json
import datetime
from random import choices
import math
from plugins.utils.utils import trigger_push_result_to_mq
from airflow.utils import timezone

_logger = LoggingMixin().log
SUPPORT_DEVICE_TYPE = ['tightening', 'servo_press']

MINIO_ROOT_URL = os.environ.get('MINIO_ROOT_URL', None)
RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

ANALYSIS_NOK_RESULTS = True if os.environ.get('ANALYSIS_NOK_RESULTS', 'False') == 'True' else False
FILTER_MISMATCHES = True if os.environ.get('FILTER_MISMATCHES', 'False') == 'True' else False
MISMATCH_RATE_RELAXATION_FACTOR = float(os.environ.get('MISMATCH_RATE_RELAXATION_FACTOR', '1'))
MISMATCH_RATE_RELAXATION_THRESHOLD = float(os.environ.get('MISMATCH_RATE_RELAXATION_THRESHOLD', '0.001'))

def is_mismatch(measure_result, curve_mode):
    analysis_result = 'OK' if curve_mode[0] is 0 else 'NOK'
    return analysis_result != measure_result


@provide_session
def get_recent_mismatch_rate(session=None):
    delta = datetime.timedelta(days=2)
    min_date = timezone.utcnow() - delta
    from qcos_addons.models.result import ResultModel
    total = session.query(ResultModel).filter(
        ResultModel.execution_date > min_date
    ).count()
    mismatches = session.query(ResultModel).filter(
        ResultModel.execution_date > min_date,
        ResultModel.measure_result != ResultModel.result
    ).count()
    _logger.info('total:{},mismatches:{}'.format(total, mismatches))
    return mismatches / (total + 1), total


def mismatch_relaxation(mismatch_rate, count) -> bool:
    if mismatch_rate < MISMATCH_RATE_RELAXATION_THRESHOLD:
        return False
    weight = MISMATCH_RATE_RELAXATION_FACTOR * (mismatch_rate - MISMATCH_RATE_RELAXATION_THRESHOLD) / (
        mismatch_rate + MISMATCH_RATE_RELAXATION_THRESHOLD) * math.log(count, 2)
    _logger.info('weight: {}'.format(weight))
    return choices([True, False], weights=[weight, 1])[0]


def filter_mismatches(measure_result, curve_mode):
    if not is_mismatch(measure_result, curve_mode):
        _logger.info('not mismatch')
        return curve_mode
    _logger.info('is mismatch')
    mismatch_rate, count = get_recent_mismatch_rate()
    _logger.info('mismatch_rate:{}, count:{}'.format(mismatch_rate, count))
    if mismatch_relaxation(mismatch_rate, count):
        return [0] if measure_result == 'OK' else [1]
    return curve_mode


class ResultStorageHook(BaseHook, ABC):

    @staticmethod
    def save_result(entity_id, result, **extra):
        _logger.info('start pushing result...')
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        if not st:
            raise Exception('result storage not ready!')
        _logger.debug('pushing result...')
        result_to_write = extra.copy()

        result_to_write.update(result)
        st.write_result(result_to_write)

    @staticmethod
    def save_curve(entity_id, curve):
        _logger.info('start pushing curve...')
        curve_args = get_curve_args()
        if MINIO_ROOT_URL:
            _logger.debug(f'override OSS URL： {MINIO_ROOT_URL}')
            curve_args.update({'endpoint': MINIO_ROOT_URL})
        ct = ClsCurveStorage(**curve_args)
        ct.metadata = {
            'entity_id': entity_id
        }  # 必须在设置curvefile前赋值
        data = {
            'curve': curve
        }
        if not ct:
            raise Exception('curve storage not ready!')
        try:
            _logger.debug(f'write curve params： {pformat(data, indent=4)}')
            ct.write_curve(data)
            _logger.info('pushing curve success')
        except Exception as e:
            _logger.error(f'writing curve error: {repr(e)}')
            raise e

    @staticmethod
    def is_valid_params(params):
        if not params:
            raise Exception(u'参数params不存在')
        result = params.get('result', None)
        if not result:
            raise Exception(u'参数params中result不存在')
        device_type = result.get('device_type', 'tightening')
        if not device_type:  # 如果是空值,设定为默认值
            device_type = 'tightening'
        if device_type not in SUPPORT_DEVICE_TYPE:
            raise Exception(u'参数result中设备类型: {}不支持'.format(device_type))
        result.update({'device_type': device_type})
        return params

    @staticmethod
    def generate_extra_data(result, should_analyze, factory_code):
        # 螺栓编码生成规则：控制器名称-job号-批次号
        controller_name = result.get('controller_name', None)
        job = result.get('job', None)
        vin = result.get('vin', None)
        batch_count = result.get('batch_count', None)
        pset = result.get('pset', None)
        bolt_number = generate_bolt_number(controller_name, job, batch_count, pset)

        try:
            craft_type = get_craft_type(bolt_number)
            _logger.info("craft_type: {}".format(craft_type))
        except Exception as e:
            _logger.error(e)
            craft_type = 1
            _logger.info('使用默认工艺类型：{}'.format(craft_type))

        try:
            from qcos_addons.models.tightening_controller import TighteningController
            line_code, controller_id = TighteningController.get_line_code_by_controller_name(controller_name)
        except Exception as e:
            _logger.error(e)
            line_code = None
            controller_id = None

        from plugins.trigger_analyze.trigger_analyze_plugin import TriggerAnalyzeHook

        return {
            'line_code': line_code,
            'factory_code': factory_code,
            'should_analyze': should_analyze,
            'bolt_number': bolt_number,
            'device_type': result.get('device_type', 'tightening'),
            'type': TriggerAnalyzeHook.get_result_type(result),
            'craft_type': craft_type,
            'controller_id': controller_id,
            'car_code': vin
        }

    @staticmethod
    def on_curve_receive(params):
        should_store = True  # 目前总是为True，未来视情况更改
        if should_store:
            entity_id = params.get('entity_id')

            should_analyze = params.get('should_analyze')
            factory_code = params.get('factory_code', None)
            extra_data = ResultStorageHook.generate_extra_data(result, should_analyze, factory_code)

            ResultStorageHook.save_result(
                entity_id,
                result,
                **extra_data
            )

            ResultStorageHook.save_curve(entity_id, params.get('curve'))
        _logger.debug(params)
        params.update({
            'bolt_number': bolt_number,
            'craft_type': craft_type
        })
        return params

    # 根据entity_id更新分析结果
    @staticmethod
    def save_analyze_result(entity_id, measure_result, curve_mode, verify_error):
        if FILTER_MISMATCHES:
            curve_mode = filter_mismatches(measure_result, curve_mode)

        result = 'OK' if curve_mode[0] is 0 else 'NOK'
        if (not ANALYSIS_NOK_RESULTS) and measure_result == 'NOK':
            result = 'NOK'

        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        st.update(
            result=result,
            error_tag= json.dumps(curve_mode if curve_mode[0] is not 0 else []),
            verify_error= int(verify_error)
        )
        trigger_push_result_to_mq(
            'analysis_result',
            result,
            entity_id,
            verify_error,
            curve_mode
        )


    # 根据entity_id更新分析二次确认结果
    @staticmethod
    def save_final_state(entity_id, final_state, **extra):
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        st.update(
            final_state=final_state,
            **extra
        )

    # 在结果中保存task相关信息
    @staticmethod
    def bind_analyze_task(entity_id, dag_id, task_id, execution_date):
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        st.update(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date
        )


# Defining the plugin class
class ResultStoragePlugin(AirflowPlugin):
    name = "result_storage_plugin"
    hooks = [ResultStorageHook]
