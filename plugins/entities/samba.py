import threading
from datetime import datetime

from qcos_addons.models.result import ResultModel
from smb.SMBConnection import *
import io

from plugins.entities.entity import ClsEntity
from plugins.utils.logger import generate_logger
import pandas as pd
from flask import Flask

app = Flask(__name__)

_logger = generate_logger(__name__)


class SmbFile(ClsEntity):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(SmbFile, "_instance"):
            with SmbFile._instance_lock:
                if not hasattr(SmbFile, "_instance"):
                    SmbFile._instance = object.__new__(cls)
        return SmbFile._instance

    def __init__(self, user_name, password, ip, port, my_name, remote_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_name = user_name
        self.passwd = password
        self.ip = ip
        self.port = port
        self.my_name = my_name
        self.remote_name = remote_name
        self._client = None
        self.status = False

    def Smb_connect(self):
        try:
            self._client = SMBConnection(self.user_name, self.passwd, self.my_name, self.remote_name, use_ntlm_v2=True)
            self._client.connect(self.ip)
            self.status = self._client.auth_result
        except Exception as e:
            self._client.close()
            raise e

    def Smb_disconnect(self):
        if self.status:
            self._client.close()

    def uploadDir(self, service_name, rs_pk):
        from qcos_addons.models.result import ResultModel
        names = ResultModel.get_names()
        pk = ResultModel.query_pk()
        file_time = str(datetime.now().strftime('%Y-%m-%d %H-%M'))
        self._client.createDirectory(service_name, file_time)
        for file_name in names:
            n = str(file_name)
            file_names = f"{file_time}/{n}.csv"
            lists = n.split('@@')
            path = f"{service_name}"
            self.uploadCsv(path, file_names, bolt_number=lists[0], craft_type=lists[1], rs_pk=rs_pk)
        return pk

    def uploadCsv(self, service_name, smb_folder, craft_type=None, bolt_number=None, rs_pk=None):
        results = ResultModel.query_result(craft_type, bolt_number, rs_pk)
        df_results = pd.DataFrame(results)
        result_buf = io.BytesIO()
        df_results.to_csv(result_buf)
        result_buf.seek(0)
        _logger.info(f'saving {len(df_results)} results to {service_name}/{smb_folder}')
        self._client.storeFile(service_name, smb_folder, result_buf)

    @staticmethod
    def get_samba_args(key='qcos_samba'):
        from airflow.models.connection import Connection
        smb = Connection.get_connection_from_secrets(key)
        if smb is None:
            _logger.error('连接"{}"未配置'.format(key))
            return {
                "host": None,
                "port": None,
                "username": None,
                "password": None
            }
        data = {
            "host": smb.host,
            "port": smb.port,
            "username": smb.login,
            "password": smb.get_password()
        }
        try:
            data.update(smb.extra_dejson)
        except Exception as e:
            _logger.error(e)
        return data
