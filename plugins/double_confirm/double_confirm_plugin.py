import datetime
from flask import jsonify, request
from flask_appbuilder import BaseView, expose
from flask_login import current_user
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import TIMEZONE
from airflow.exceptions import AirflowException
from plugins.utils.utils import trigger_training_dag, get_result
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.app import csrf
from airflow.security import permissions
from qcos_addons.access_log.log import access_log

_log = LoggingMixin().log


class DoubleConfirmView(BaseView):
    """
    二次确认
    """
    route_base = ''

    base_permissions = [permissions.ACTION_CAN_EDIT]

    class_permission_name = permissions.RESOURCE_RESULT

    method_permission_name = {
        'double_confirm_task': 'edit',
    }

    base_permissions = [
        permissions.ACTION_CAN_READ
    ]

    @expose('/double-confirm/<string:entity_id>', methods=['POST'])
    @access_log('DOUBLE_CONFIRM', 'CURVE', '曲线二次确认')
    def double_confirm_task(self, entity_id):
        """
        触发二次确认接口
        @param entity_id: 曲线编号
        @return:
        """
        try:
            # 从请求中获取数据
            params = request.get_json(force=True)  # success failed
            final_state = params.get('final_state', None)
            error_tags = params.get('error_tags', [])
            entity_id = entity_id.replace('@', '/')
            result = get_result(entity_id)
            # 参数校验
            if not result.get('result'):
                raise AirflowException(u"分析结果还没有生成，请等待分析结果生成后再进行二次确认")
            if not final_state or final_state not in ['OK', 'NOK']:
                raise AirflowException("二次确认参数未定义或数值不正确!")
            # 触发训练dag
            trigger_training_dag(entity_id, final_state, error_tags)
            return jsonify({'response': 'ok'})
        except AirflowException as err:
            _log.info(err)
            response = jsonify(error="{}".format(err))
            response.status_code = err.status_code
            return response


double_confirm_view = DoubleConfirmView()
double_confirm_view_package = {"view": double_confirm_view}


# Defining the plugin class
class DoubleConfirmPlugin(AirflowPlugin):
    name = "double_confirm_plugin"
    appbuilder_views = [double_confirm_view_package]
