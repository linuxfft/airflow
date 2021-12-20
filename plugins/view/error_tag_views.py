# -*- coding: utf-8 -*-

import json
from plugins.common import AirflowModelView
from datetime import datetime
from flask_login import current_user
from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
from wtforms.fields import StringField
from flask_appbuilder.forms import DynamicForm
from flask_babel import lazy_gettext
from flask_appbuilder.actions import action
from flask_appbuilder import expose
from flask import make_response, redirect
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import TIMEZONE
from airflow.utils.db import provide_session
from flask_appbuilder.models.sqla.filters import BaseFilter, get_field_setup_query
from plugins.models.error_tag import ErrorTag
import logging
import pprint
from airflow.security import permissions

_logger = logging.getLogger(__name__)


class ErrorTagFilter(BaseFilter):

    def apply(self, query, func):  # noqa
        _logger.info("ErrorTagFilter: {}".format(pprint.pformat(func)))
        query, field = get_field_setup_query(query, self.model, self.column_name)
        return query


class ErrorTagForm(DynamicForm):
    value = StringField(
        lazy_gettext('Value'),
        widget=BS3TextFieldWidget())
    label = StringField(
        lazy_gettext('Label'),
        widget=BS3TextFieldWidget())


class ErrorTagModelView(AirflowModelView):
    route_base = '/error_tag'

    datamodel = AirflowModelView.CustomSQLAInterface(ErrorTag)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = []
    list_columns = ['value', 'label']
    add_columns = edit_columns = ['value', 'label'] + extra_fields
    add_form = edit_form = ErrorTagForm
    add_template = 'error_tag_create.html'
    edit_template = 'error_tag_edit.html'
    label_columns = {
        'value': lazy_gettext('Value'), 'label': lazy_gettext('Label')

    }

    class_permission_name = permissions.RESOURCE_ERROR_TAG

    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        permissions.ACTION_CAN_EDIT,
        permissions.ACTION_CAN_ACCESS_MENU,
        permissions.ACTION_CAN_DELETE
    ]

    method_permission_name = {
        'action_export_error_tag_statistics': 'read',
        'action_muldelete': 'delete',
        'list': 'read',
        'show': 'read',
        'add': 'create',
    }
    base_order = ('id', 'asc')

    @access_log('ADD', 'ERROR_TAG', '增加错误标签')
    def post_add(self, item):
        super(ErrorTagModelView, self).post_add(item)

    @access_log('UPDATE', 'ERROR_TAG', '修改错误标签')
    def post_update(self, item):
        super(ErrorTagModelView, self).post_update(item)

    @access_log('DELETE', 'ERROR_TAG', '删除错误标签')
    def post_delete(self, item):
        super(ErrorTagModelView, self).post_delete(item)

    @action('export_analysis', "Export Statistics", '', single=False)
    @provide_session
    def action_export_error_tag_statistics(self, error_tags, session=None):
        ret = {}
        d = json.JSONDecoder()
        for var in error_tags:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            ret[var.key] = val

        response = make_response(json.dumps(ret, sort_keys=True, indent=4, ensure_ascii=False))
        response.headers["Content-Disposition"] = "attachment; filename=错误标签分析.json"
        response.headers["Content-Type"] = "application/json; charset=utf-8"
        return response

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @access_log('DELETE', 'ERROR_TAG', '删除选中错误标签')
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    # 重写list
    @expose("/list/")
    @access_log('VIEW', 'ERROR_TAG', '查看错误标签')
    def list(self):
        return super(ErrorTagModelView, self).list()


error_tag_view = ErrorTagModelView()
error_tag_package = {"name": permissions.RESOURCE_ERROR_TAG,
                     "category": permissions.RESOURCE_MASTER_DATA_MANAGEMENT,
                     "view": error_tag_view}


class ErrorTagViewPlugin(AirflowPlugin):
    name = "error_tag_view"
    appbuilder_views = [error_tag_package]
