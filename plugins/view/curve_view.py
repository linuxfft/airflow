from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.plugins_manager import AirflowPlugin
import logging
from flask_appbuilder import BaseView, expose
from airflow.models import Variable
from plugins.utils.utils import get_curve, get_result
from qcos_addons.models.error_tag import ErrorTag
import os
from airflow.security import permissions
from qcos_addons.access_log.log import access_log
import json

_logger = logging.getLogger(__name__)


class CurveView(BaseView):
    route_base = ''

    base_permissions = [permissions.ACTION_CAN_READ]

    class_permission_name = permissions.RESOURCE_CURVE

    method_permission_name = {
        'view_curve_page': 'read'
    }

    base_permissions = [
        permissions.ACTION_CAN_READ
    ]

    @expose('/view_curve/<string:entity_id>')
    @access_log('VIEW', 'CURVE', '查看单条曲线')
    def view_curve_page(self, entity_id: str):
        entity_id = entity_id.replace('@', '/')

        _has_access = self.appbuilder.sm.has_access

        try:
            result = get_result(entity_id)
        except Exception as e:
            _logger.error(e)
            result = {}
        try:
            curve = get_curve(entity_id)
        except Exception as e:
            _logger.error(e)
            curve = {}

        analysis_error_message_mapping = Variable.get('analysis_error_message_mapping', deserialize_json=True,
                                                      default_var={})

        verify_error_map = Variable.get('verify_error_map', deserialize_json=True,
                                        default_var={})

        result_error_message_mapping = Variable.get('result_error_message_mapping', deserialize_json=True,
                                                    default_var={})

        controller_name = result.get('controller_name', '').split('@')[0] if result.get('controller_name') else ''
        from qcos_addons.models.tightening_controller import TighteningController
        controller = TighteningController.find_controller(controller_name)
        error_tags = ErrorTag.get_all()
        ENV_CURVE_GRAPH_SHOW_RANGE = os.environ.get('CURVE_GRAPH_SHOW_RANGE', False)
        show_range = (ENV_CURVE_GRAPH_SHOW_RANGE is True) or (ENV_CURVE_GRAPH_SHOW_RANGE == 'True')
        can_verify = _has_access(permissions.ACTION_CAN_EDIT, permissions.RESOURCE_RESULT)
        view_config = json.loads(controller.device_type.view_config) \
            if controller.device_type.view_config is not None else {}
        cur_key_map = view_config.get('curve_key_map', {})
        cur_unit_map = view_config.get('curve_unit_map', {})
        display_keys = view_config.get('display_keys', {})
        translation_mapping = view_config.get('translation_mapping', {})

        return self.render_template('curve.html', result=result,
                                    curve=curve, analysisErrorMessageMapping=analysis_error_message_mapping,
                                    resultErrorMessageMapping=result_error_message_mapping,
                                    resultKeysTranslationMapping=translation_mapping,
                                    verify_error_map=verify_error_map,
                                    can_verify=can_verify,
                                    controller=controller.to_dict() if controller else {},
                                    errorTags=error_tags,
                                    show_range=show_range,
                                    display_keys=display_keys,
                                    cur_key_map=cur_key_map,
                                    cur_unit_map=cur_unit_map
                                    )


curve_view = CurveView()
curve_view_package = {"view": curve_view}


class CurveViewPlugin(AirflowPlugin):
    name = "curve_view_plugin"
    appbuilder_views = [curve_view_package]
