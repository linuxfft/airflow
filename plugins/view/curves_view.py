from flask_appbuilder.urltools import get_filter_args, get_page_args
import http
import json
from flask import Response
from flask import send_file
from flask_appbuilder import expose
from flask_babel import lazy_gettext
from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from airflow.configuration import conf
from airflow.exceptions import AirflowNotFoundException
from qcos_addons.models.error_tag import ErrorTag
from airflow.www import utils as wwwutils
from plugins.utils.utils import get_curve_entity_ids, get_results, get_curves
import logging
from qcos_addons.download.CurveResultDownloader import CurveResultDownloader
from airflow.plugins_manager import AirflowPlugin
from plugins.common import AirflowModelView
from flask import jsonify, request
from airflow.exceptions import AirflowException
from airflow.security import permissions
from qcos_addons.access_log.log import access_log
from os import environ

_logger = logging.getLogger(__name__)

PAGE_SIZE = conf.getint('webserver', 'page_size')


class CurvesView(AirflowModelView):
    list_template = "curves.html"
    CustomSQLAInterface = wwwutils.CustomSQLAInterface
    route_base = '/curves'
    from qcos_addons.models.result import ResultModel
    datamodel = CustomSQLAInterface(ResultModel)
    search_columns = ['update_time', 'car_code', 'error_tag', 'measure_result', 'result', 'final_state']
    label_columns = {
        'error_tag': lazy_gettext('Error Tags'),
        'update_time': lazy_gettext('Update Time'),
        'car_code': lazy_gettext(environ.get('ENV_CAR_CODE_TEXT', 'Car Code')),
        'measure_result': lazy_gettext('Measure Result'), 'result': lazy_gettext('Result'),
        'final_state': lazy_gettext('Final State')
    }

    class_permission_name = permissions.RESOURCE_CURVES

    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    method_permission_name = {
        'view_curves_analysis': 'read',
        'download': 'read',
        'view_curves': 'read',
        'get_curves_by_entity_id': 'read',
        'get_curves': 'read'
    }

    def __init__(self, *args, **kwargs):
        ret = super(CurvesView, self).__init__(**kwargs)

    @access_log('VIEW', 'CURVES', '查看曲线对比页面')
    def do_render(self, track_no=None, bolt_no=None, controller=None, craft_type=None):
        view_name = 'curves'
        curves = request.args.get('curves')
        curves_list = curves.replace('@', '/').split(',') if curves is not None else []
        pages = get_page_args()
        page = pages.get(view_name, 0)
        get_filter_args(self._filters)
        if bolt_no:
            self._filters.add_filter(column_name='bolt_number', filter_class=self.datamodel.FilterEqual, value=bolt_no)
        if craft_type:
            self._filters.add_filter(column_name='craft_type', filter_class=self.datamodel.FilterEqual,
                                     value=int(craft_type))
        if track_no:
            self._filters.add_filter(column_name='car_code', filter_class=self.datamodel.FilterEqual, value=track_no)
        if controller:
            self._filters.add_filter(column_name='controller_name', filter_class=self.datamodel.FilterContains,
                                     value=controller)

        joined_filters = self._filters.get_joined_filters(self._base_filters)
        order_column, order_direction = "update_time", "desc"
        page_size = PAGE_SIZE
        count, lst = self.datamodel.query(
            joined_filters,
            order_column,
            order_direction,
            page=page,
            page_size=page_size,
        )

        error_tag_vals = ErrorTag.get_all_dict() or {}
        view_config = None
        for t in lst:
            ret = []
            if view_config is None \
                and t.controller is not None \
                and t.controller.device_type is not None \
                and t.controller.device_type.view_config is not None:
                view_config = t.controller.device_type.view_config
            try:
                error_tags = json.loads(t.error_tag or '[]')
                if not error_tags:
                    t.view_error_tags = u'无异常标签'
                    continue
                for tag in error_tags:
                    v = error_tag_vals.get(str(tag), '')
                    if not v:
                        continue
                    ret.append(v)
            except Exception as e:
                t.view_error_tags = ','.join(ret)
            t.view_error_tags = ','.join(ret)

        selected_results = {}
        results = list(get_results(curves_list))

        for result in results:
            selected_results[result.get('entity_id')] = {
                'carCode': result.get('car_code'),
                'value': result.get('entity_id'),
                'date': str(result.get('update_time'))
            }
        widgets = self._list()
        if view_config is None:
            try:
                from qcos_addons.constants import ENV_DEFAULT_DEVICE_VIEW_CONFIG
                view_config = ENV_DEFAULT_DEVICE_VIEW_CONFIG
            except Exception as e:
                _logger.error(e)
        cur_key_map = json.loads(view_config).get('curve_key_map') if view_config is not None else {}
        cur_unit_map = json.loads(view_config).get('curve_unit_map') if view_config is not None else {}

        return self.render_template('curves.html', results=lst, page=page, page_size=page_size, count=count,
                                    modelview_name=view_name,
                                    selected_curves=curves_list,
                                    selected_results=selected_results,
                                    cur_key_map=cur_key_map,
                                    cur_unit_map=cur_unit_map,
                                    car_code_name=environ.get('ENV_CAR_CODE_TEXT', 'Car Code'),
                                    widgets=widgets)

    @expose('/analysis')
    def view_curves_analysis(self):
        track_no = request.args.get('track_no', default=None)
        bolt_no = request.args.get('bolt_no', default=None)
        controller = request.args.get('controller', default=None)
        ret = None
        if track_no:
            ret = self.do_render(track_no=track_no)
        elif bolt_no:
            ret = self.do_render(bolt_no=bolt_no)
        elif controller:
            ret = self.do_render(controller=controller)
        if not ret:
            raise AirflowNotFoundException
        return ret

    @expose('/download/<string:entity_ids>')
    def download(self, entity_ids: str):
        if not entity_ids or entity_ids == 'None':
            return Response(status=http.HTTPStatus.OK)
        entity_ids = entity_ids.replace('@', '/')
        entities = entity_ids.split(',')
        fn = CurveResultDownloader.prepare_download_file(entities)
        return send_file(fn, mimetype='application/zip', attachment_filename='curves.zip',
                         as_attachment=True)

    @expose('/<string:bolt_no>/<string:craft_type>')
    def view_curves(self, bolt_no, craft_type):
        ret = self.do_render(bolt_no=bolt_no, craft_type=craft_type)
        return ret

    @expose(
        '/curves',
        methods=['GET'])
    def get_curves_by_entity_id(self):
        try:
            curves = []

            vals = request.args.get('entity_ids')
            entity_ids = str(vals).split(",")
            if entity_ids is None:
                return jsonify(curves)

            try:
                curves = get_curves(entity_ids)
            except Exception as e:
                _logger.error(e)

            return jsonify(curves=curves)
        except AirflowException as e:
            _logger.error("get_curves_by_entity_id", e)
            response = jsonify(error="{}".format(repr(e)))
            response.status_code = e.status_code
            return response

    @expose('/curve-entities', methods=['GET'])
    def get_curves(self):
        try:
            craft_type = request.args.get('craft_type')
            bolt_number = request.args.get('bolt_number')
            entity_ids = get_curve_entity_ids(bolt_number, craft_type)
            return jsonify(entity_ids)
        except AirflowException as e:
            _logger.error(e)
            response = jsonify(error="{}".format(e))
            response.status_code = e.status_code
            return response


curves_view = CurvesView()
curves_view_package = {"view": curves_view}


class CurvesViewPlugin(AirflowPlugin):
    name = "curves_view_plugin"
    appbuilder_views = [curves_view_package]
