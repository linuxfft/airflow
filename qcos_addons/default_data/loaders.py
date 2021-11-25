from qcos_addons.utils.load_data_from_csv import load_data_from_csv
import os
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log


def _do_import(sub_path, data_keys=None):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, sub_path)
    if not os.path.exists(file_path):
        log.error("目录不存在：{}".format(file_path))
        return []
    return load_data_from_csv(file_path, data_keys)


def load_default_controllers(factory):
    return _do_import(
        f'data/{factory}/default_controllers.csv' if factory else 'data/default_controllers.csv'
    )


def load_default_error_tags():
    return _do_import(
        'data/error_tags.csv',
        {
            'value': 'value',
            'label': 'label'
        }
    )


def load_default_device_types():
    return _do_import(
        'data/device_types.csv',
        {
            'name': 'name',
            'view_config': 'view_config'
        }
    )


def load_default_users(factory):
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
