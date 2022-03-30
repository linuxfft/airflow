import threading
from datetime import timedelta, datetime

from flask_sqlalchemy import SQLAlchemy
from pika.compat import time_now
from smb.SMBConnection import *
import io

from sqlalchemy import create_engine

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

    def __init__(self, user_name, password, ip, port, my_name, remote_name):
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

    def uploadDir(self, service_name):
        from qcos_addons.models.result import ResultModel
        names = ResultModel.get_names()
        file_time = str(datetime.now().strftime('%Y-%m-%d %H-%M'))
        for file_name in names:
            try:
                n = str(file_name)
                n = n.replace('/', '@@')
                file_names = f"{file_time}\\{n}.csv"
                lists = n.split('/')
                self._client.createDirectory(service_name, file_names)
                self.uploadCsv(service_name, file_names, craft_type=lists[0], bolt_number=lists[1])
            except OperationFailure as e:
                raise e



    def openPath(self, localPath):
        localFile = open(localPath, "rb")
        return localFile

    def uploadCsv(self, service_name, smb_folder, craft_type=None, bolt_number=None):
        from qcos_addons.models.result import ResultModel
        results = ResultModel.query_results(craft_type, bolt_number).all
        result = results.filter(ResultModel.update_time >= time_now - timedelta(hours=4)).all()
        df_results = pd.DataFrame(result)
        result_buf = io.StringIO()
        df_results.to_csv(result_buf)
        # local_file = self.openPath(localPath)
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
        login = smb.login
        password = smb.get_password()
        host = smb.host
        port = smb.port
        extra = smb.get_extra()
        try:
            data.update(smb.extra_dejson)
        except Exception as e:
            _logger.error(e)
        return login, password, host, port, extra


