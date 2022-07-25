from typing import List
from qcos_addons.models.result import ResultModel
import json
from plugins.common import AirflowModelView
from flask_babel import lazy_gettext
from airflow.plugins_manager import AirflowPlugin
from qcos_addons.models.error_tag import ErrorTag
from airflow.configuration import conf
from airflow.security import permissions
from flask_appbuilder import expose
from flask import redirect, abort, url_for
from os import environ
from flask_appbuilder.actions import action
from qcos_addons.download.CurveResultDownloader import CurveResultDownloader
from flask import send_file

import pytz
from datetime import datetime

PAGE_SIZE = conf.getint('webserver', 'page_size')


class ResultModelView(AirflowModelView):
    route_base = '/results'

    datamodel = AirflowModelView.CustomSQLAInterface(ResultModel)

    page_size = PAGE_SIZE

    list_columns = [
        'entity_id',
        'update_time',
        'car_code',
        'controller_name',
        'tool_sn',
        'pset',
        'job',
        'batch_count',
        'bolt_number',
        'craft_type',
        'measure_result',
        'measure_torque',
        'measure_angle',
        'result',
        'final_state'
    ]

    search_columns = [
        'entity_id',
        'update_time',
        'car_code',
        'controller_name',
        'tool_sn',
        'pset',
        'job',
        'batch_count',
        'bolt_number',
        'craft_type',
        'measure_result',
        'measure_torque',
        'measure_angle',
        'result',
        'final_state'
    ]

    label_columns = {
        'entity_id': lazy_gettext('Entity Id'),
        'update_time': lazy_gettext('Update Time'),
        'car_code': lazy_gettext(environ.get('ENV_CAR_CODE_TEXT', 'Car Code')),
        'controller_name': lazy_gettext('Controller Name'),
        'tool_sn': lazy_gettext('Tool SN'),
        'pset': lazy_gettext('PSET'),
        'job': lazy_gettext('Job'),
        'batch_count': lazy_gettext('Batch Count'),
        'bolt_number': lazy_gettext('Bolt Number'),
        'craft_type': lazy_gettext('Craft Type'),
        'measure_result': lazy_gettext('Measure Result'),
        'measure_torque': lazy_gettext('Measure Torque'),
        'measure_angle': lazy_gettext('Measure Angle'),
        'result': lazy_gettext('Result'),
        'final_state': lazy_gettext('Final State'),
    }

    base_order = ('update_time', 'desc')

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_ACCESS_MENU
    ]

    method_permission_name = {
        'list': 'read',
        'show': 'read',
        'edit': 'edit_all',
        'action_export_results': 'read'
    }

    class_permission_name = permissions.RESOURCE_RESULT

    # base_filters = [['dag_id', DagFilter, lambda: []]]

    def error_tag_f(attr):
        ret = []
        try:
            error_tags = json.loads(attr.get('error_tag') or '[]')
            if not error_tags:
                return u'无异常标签'
            error_tag_vals = ErrorTag.get_all_dict() or {}
            for tag in error_tags:
                v = error_tag_vals.get(str(tag), '')
                if not v:
                    continue
                ret.append(v)
        except Exception as e:
            return ','.join(ret)
        return ','.join(ret)

    def update_time_f(attr):
        update_time = attr.get('update_time')

        try:
            tz = pytz.timezone("Asia/Shanghai")

            if isinstance(update_time, str):
                tz_time = datetime.strptime(update_time, "%Y-%m-%d %H:%M:%S%z")
                tz_time = tz_time.astimezone(tz)

                return tz_time
            elif isinstance(update_time, datetime):
                tz_time = update_time.astimezone(tz)

                return tz_time

            else:
                return update_time

        except Exception as e:
            return update_time

    formatters_columns = {
        'error_tag': error_tag_f,
        'update_time': update_time_f,
    }

    @expose("/show/<pk>", methods=["GET"])
    def show(self, pk):
        pk = self._deserialize_pk_if_composite(pk)
        item = self.datamodel.get(pk, self._base_filters)
        if not item:
            abort(404)
        return redirect(url_for('CurveView.view_curve_page', entity_id=item.entity_id.replace('/', '@')))

    @action('export_results', lazy_gettext("Export"), '', single=False)
    def action_export_results(self, results: List[ResultModel]):
        fn = CurveResultDownloader.prepare_download_file(results=results)
        return send_file(fn, mimetype='application/zip', attachment_filename='curves.zip',
                         as_attachment=True)


result_view = ResultModelView()
result_view_package = {"name": permissions.RESOURCE_RESULT,
                       "category": permissions.RESOURCE_ANALYSIS,
                       "view": result_view}


class ResultViewPlugin(AirflowPlugin):
    name = "result_view"
    appbuilder_views = [result_view_package]
