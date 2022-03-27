from smb.SMBConnection import *

from plugins.entities.entity import ClsEntity
from plugins.utils.logger import generate_logger

_logger = generate_logger(__name__)

class SmbFile(ClsEntity):

    def _init_(self, user_name, password, ip, port, my_name, remote_name):
        self.user_name = user_name
        self.passwd = password
        self.ip = ip
        self.port = port
        self.my_name = my_name
        self.remote_name = remote_name
        self._client = None

    def Smb_connect(self):
        try:
            conn = SMBConnection(self.user_name, self.passwd, self.my_name, self.remote_name, use_ntlm_v2=True)
            self._client = conn.connect(self.ip, self.port)
            self.status = self._client.auth_result
        except:
            self._client.close()

    def Smb_disconnect(self):
        if self.status:
            self._client.close()

    def createDir(self, service_name, path):
        """
           创建文件夹
           :param service_name:共享空间(如C$或/Share/)
           :param path:
           :return:
           """
        try:
            self._client.createDirectory(service_name, path)
        except OperationFailure:
            pass

    def openPath(self, localPath):
        localFile = open(localPath, "rb")
        return localFile

    def upload(self, file_name, smb_path, localPath):
        local_file = self.openPath(localPath)
        self._client.storeFile(file_name, smb_path, local_file)

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
        h = smb.host
        p = smb.port
        lo = smb.login
        ps = smb.get_password()
        try:
            data.update(smb.extra_dejson)
        except Exception as e:
            _logger.error(e)
        return h, p, lo, ps


