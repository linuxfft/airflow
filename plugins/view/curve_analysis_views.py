# -*- coding: utf-8 -*-

from plugins.common import PAGE_SIZE, AirflowModelView
from airflow.www.widgets import AirflowModelListWidget
from flask_babel import lazy_gettext
from airflow.www import utils as wwwutils
from flask_appbuilder.models.filters import BaseFilter

from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from os import environ


class CurveAnalysisListWidget(AirflowModelListWidget):
    template = 'curve_analysis_list.html'


class TrackNoNotNullFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        result = self.model
        ret = query.filter(result.car_code.isnot(None)).distinct(result.car_code)
        return ret


class BoltNoNotNullFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        result = self.model
        return query.filter(result.bolt_number.isnot(None)).distinct(result.bolt_number)


class CurveAnalysisBaseView(AirflowModelView):
    list_widget = CurveAnalysisListWidget
    page_size = PAGE_SIZE
    method_permission_name = {
        'list': 'read',
        'show': 'read'
    }
    base_permissions = [
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_ACCESS_MENU
    ]
    to_curve_templates = False
    to_curves = False
    to_results = False

    def _get_templates_filters(self, item, *args, **kwargs):
        return {'bolt_no': None, 'craft_type': None}

    def _get_curves_filters(self, item, *args, **kwargs):
        return {}

    def _get_results_filters(self, item, *args, **kwargs):
        return {}

    def _get_list_widget(
        self,
        filters,
        actions=None,
        order_column="",
        order_direction="",
        page=None,
        page_size=None,
        widgets=None,
        **args,
    ):
        """ get joined base filter and current active filter for query """
        widgets = widgets or {}
        actions = actions or self.actions
        page_size = page_size or self.page_size
        if not order_column and self.base_order:
            order_column, order_direction = self.base_order
        joined_filters = filters.get_joined_filters(self._base_filters)
        count, lst = self.datamodel.query(
            joined_filters,
            order_column,
            order_direction,
            page=page,
            page_size=page_size,
        )
        pks = self.datamodel.get_keys(lst)

        # serialize composite pks
        pks = [self._serialize_pk_if_composite(pk) for pk in pks]

        widgets["list"] = self.list_widget(
            label_columns=self.label_columns,
            include_columns=self.list_columns,
            value_columns=self.datamodel.get_values(lst, self.list_columns),
            order_columns=self.order_columns,
            formatters_columns=self.formatters_columns,
            page=page,
            page_size=page_size,
            count=count,
            pks=pks,
            actions=actions,
            filters=filters,
            modelview_name=self.__class__.__name__,
            to_curve_templates=self.to_curve_templates,
            to_curves=self.to_curves,
            to_results=self.to_results,
            get_templates_filters=self._get_templates_filters,
            get_curves_filters=self._get_curves_filters,
            get_results_filters=self._get_results_filters,
        )
        return widgets


class CurveAnalysisControllerView(CurveAnalysisBaseView):
    route_base = '/curves_analysis_controller'

    from qcos_addons.models.tightening_controller import TighteningController
    datamodel = AirflowModelView.CustomSQLAInterface(TighteningController)
    list_title = lazy_gettext("Analysis Via Controller")

    class_permission_name = permissions.RESOURCE_ANALYSIS_VIA_CONTROLLER

    list_columns = ['controller_name', 'device_type']

    to_curves = False
    to_results = True

    def _get_results_filters(self, item, *args, **kwargs):
        return {'_flt_3_controller_name': item['controller_name']}


class CurveAnalysisTrackNoView(CurveAnalysisBaseView):
    route_base = '/curves_analysis_track'
    from qcos_addons.models.result import ResultModel
    datamodel = wwwutils.CustomSQLAInterface(ResultModel)

    page_size = PAGE_SIZE

    class_permission_name = permissions.RESOURCE_ANALYSIS_VIA_TRACK_NO

    list_title = lazy_gettext("Analysis Via Track No")

    list_columns = ['car_code']

    search_columns = ['car_code']

    label_columns = {
        'car_code': lazy_gettext(environ.get('ENV_CAR_CODE_TEXT', 'Car Code'))
    }

    base_filters = [['car_code', TrackNoNotNullFilter, lambda: []]]

    base_order = ('car_code', 'asc')

    to_results = True

    def _get_results_filters(self, item, *args, **kwargs):
        return {
            '_flt_3_car_code': item['car_code']
        }


class CurveAnalysisBoltNoView(CurveAnalysisTrackNoView):
    route_base = '/curves_analysis_bolt'
    from qcos_addons.models.result import ResultModel
    datamodel = wwwutils.CustomSQLAInterface(ResultModel)
    list_title = lazy_gettext("Analysis Via Bolt No")

    list_columns = ['bolt_number', 'craft_type']

    search_columns = ['bolt_number']

    label_columns = {
        'bolt_number': lazy_gettext('Bolt Number')
    }

    class_permission_name = permissions.RESOURCE_ANALYSIS_VIA_BOLT_NO

    base_filters = [['bolt_number', BoltNoNotNullFilter, lambda: []]]

    base_order = ('bolt_number', 'asc')

    to_curve_templates = True
    to_curves = True
    to_results = True

    def _get_templates_filters(self, item, *args, **kwargs):
        return {
            'bolt_no': item['bolt_number'], 'craft_type': item['craft_type']
        }

    def _get_curves_filters(self, item, *args, **kwargs):
        return {
            'bolt_no': item['bolt_number']
        }

    def _get_results_filters(self, item, *args, **kwargs):
        return {
            '_flt_3_bolt_number': item['bolt_number'],
            '_flt_0_craft_type': item['craft_type']
        }


curve_ana_controller_view = CurveAnalysisControllerView()
curve_ana_controller_package = {"name": permissions.RESOURCE_ANALYSIS_VIA_CONTROLLER,
                                "category": permissions.RESOURCE_ANALYSIS,
                                "view": curve_ana_controller_view}

curve_ana_track_no_view = CurveAnalysisTrackNoView()
curve_ana_track_no_package = {"name": permissions.RESOURCE_ANALYSIS_VIA_TRACK_NO,
                              "category": permissions.RESOURCE_ANALYSIS,
                              "view": curve_ana_track_no_view}

curve_ana_bolt_no_view = CurveAnalysisBoltNoView()
curve_ana_bolt_no_package = {"name": permissions.RESOURCE_ANALYSIS_VIA_BOLT_NO,
                             "category": permissions.RESOURCE_ANALYSIS,
                             "view": curve_ana_bolt_no_view}


class CurveAnalysisControllerViewPlugin(AirflowPlugin):
    name = "curve_analysis_controller_view"
    appbuilder_views = [
        curve_ana_controller_package,
        curve_ana_track_no_package,
        curve_ana_bolt_no_package
    ]
