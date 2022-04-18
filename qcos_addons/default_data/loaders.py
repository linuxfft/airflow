from qcos_addons.utils.load_data_from_csv import load_data_from_csv
from qcos_addons.utils.load_data_from_json import load_data_from_json
import os
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def _do_import(sub_path, data_keys=None):
    '''
    将传入的子路径补充为相对于此路径的完整路径，读取对应csv文件内容并返回
    '''
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, sub_path)
    if not os.path.exists(file_path):
        log.error("目录不存在：{}".format(file_path))
        return []
    return load_data_from_csv(file_path, data_keys)

def _do_import_json(sub_path):
    '''
    将传入的子路径补充为相对于此路径的完整路径，读取对应json文件内容并返回
    '''
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, sub_path)
    if not os.path.exists(file_path):
        log.error("目录不存在：{}".format(file_path))
        return {}
    return load_data_from_json(file_path)


def load_default_controllers(factory):
    '''
    读取并返回默认控制器
    '''
    return _do_import(
        f'data/{factory}/default_controllers.csv' if factory else 'data/default_controllers.csv'
    )


def load_default_error_tags():
    '''
    读取并返回默认异常标签
    '''
    return _do_import(
        'data/error_tags.csv',
        {
            'value': 'value',
            'label': 'label'
        }
    )


def load_default_device_types():
    '''
    读取并返回默认设备类型
    '''
    return _do_import(
        'data/device_types.csv',
        {
            'name': 'name',
            'view_config': 'view_config'
        }
    )


def load_default_users(factory):
    '''
    读取并返回默认用户
    '''
    return _do_import(
        f'data/{factory}/default_users.csv' if factory else 'data/default_users.csv',
        {
            'username': 'username',
            'email': 'email',
            'lastname': 'lastname',
            'firstname': 'firstname',
            'password': 'password',
            'role': 'role'
        }
    )

def load_default_curve_templates():
    '''
    读取并返回默认曲线模板
    '''
    return _do_import_json('data/default_curve_templates.json')

def load_default_variables():
    '''
    读取并返回默认airflow变量
    '''
    return _do_import_json('data/default_variables.json')
