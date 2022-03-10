import io
import json
import uuid
from typing import Dict, Optional

import urllib3
from minio import Minio, ResponseError
from minio.error import BucketAlreadyOwnedByYou, BucketAlreadyExists
from tablib import Dataset

from plugins.entities.entity import ClsEntity
from plugins.utils.utils import get_curve_template_name
from qcos_addons.models import CurveTemplateModel

from plugins.utils.logger import generate_logger
import threading

_logger = generate_logger(__name__)


class ClsTmplStorage(ClsEntity):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsTmplStorage, "_instance"):
            with ClsTmplStorage._instance_lock:
                if not hasattr(ClsTmplStorage, "_instance"):
                    ClsTmplStorage._instance = object.__new__(cls)
        return ClsTmplStorage._instance

    def __init__(self, endpoint, access_key, secret_key, secure, bucket):
        super(ClsTmplStorage, self).__init__()
        if not self.is_config_changed(endpoint, access_key, secret_key, secure, bucket):
            return
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure
        self._url = endpoint
        self._bucket = bucket
        self._fileName = ""  # type: str
        self._client = None  # type: Optional[Minio]

    def is_config_changed(self, endpoint, access_key, secret_key, secure, bucket):
        try:
            if self._access_key != access_key:
                return True
            if self._secret_key != secret_key:
                return True
            if self._secure != secure:
                return True
            if self._url != endpoint:
                return True
            if self._bucket != bucket:
                return True
            return False
        except Exception as e:
            return True

    @property
    def endpoint(self):
        return self._url

    def get_templates_from_variables(self, template_names=None) -> Dict:
        # 如果没有指定template_names，加载所有模板
        if not template_names or len(template_names) == 0:
            return CurveTemplateModel.get_all_active_curve_tmpls()
        # 加载指定模板
        templates = {}
        for t in template_names:
            key, val = CurveTemplateModel.get_fuzzy_active(t, deserialize_json=True, default_var=None)
            template_name = get_curve_template_name(key)
            templates[template_name] = val
        return templates

    def connect(self):
        if not self.endpoint:
            raise BaseException(u'{} 地址未定义'.format(__class__.__name__))
        self._client = Minio(self.endpoint,
                             access_key=self._access_key,
                             secret_key=self._secret_key,
                             secure=self._secure,
                             http_client=urllib3.PoolManager(
                                 timeout=20.0,
                                 retries=urllib3.Retry(
                                     total=5,
                                     backoff_factor=0.2,
                                     status_forcelist=[500, 502, 503, 504]
                                 )
                             )
                             )

    def ensure_connect(self):
        if not self._client:
            self.connect()

    def ensure_bucket(self, bucket):
        self.ensure_connect()
        try:
            self._client.make_bucket(bucket)
        except BucketAlreadyOwnedByYou:
            pass
        except BucketAlreadyExists:
            pass
        except ResponseError as err:
            raise err

    def count_tmpl(self, template: Dict):
        file_names = list(template.keys())
        lth = len(file_names)
        return lth

    def write_tmpl(self, template: Dict):
        # template_names = template.get('template_names', None)
        # _logger.debug('kwargs: {0}'.format(template))
        # objects = self.get_templates_from_variables(template_names)
        file_names = list(template.keys())
        lth = len(file_names)
        self.ensure_bucket(self._bucket)
        _logger.debug('bucket确认完毕，正在写入')
        for i in range(lth):
            data = json.dumps(template[file_names[i]])
            tmpl = data.encode('utf-8')
            f = io.BytesIO(tmpl)
            self._client.put_object(self._bucket, file_names[i], f, length=len(data))
        _logger.info('写入完成!')

    def write_tmpl_single(self, template: Dict):
        # template_names = template.get('template_names', None)
        # _logger.debug('kwargs: {0}'.format(template))
        # objects = self.get_templates_from_variables(template_names)
        file_names = template.keys()
        self.ensure_bucket(self._bucket)
        _logger.debug('bucket确认完毕，正在写入')
        data = json.dumps(template)
        tmpl = data.encode('utf-8')
        f = io.BytesIO(tmpl)
        self._client.put_object(self._bucket, file_names, f, length=len(data))
        _logger.info('写入完成!')
