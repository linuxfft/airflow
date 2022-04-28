# -*- coding:utf-8 -*-

import io
import urllib3
from typing import Dict, Optional, List
from tablib import Dataset
import uuid
from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from plugins.entities.entity import ClsEntity
from plugins.utils.logger import generate_logger
import threading
import concurrent.futures

_logger = generate_logger(__name__)


class ClsCurveStorage(ClsEntity):
    """
    曲线保存
    """
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(ClsCurveStorage, "_instance"):
            with ClsCurveStorage._instance_lock:
                if not hasattr(ClsCurveStorage, "_instance"):
                    ClsCurveStorage._instance = object.__new__(cls)
        return ClsCurveStorage._instance

    def __init__(self, endpoint, access_key, secret_key, secure, bucket):
        super(ClsCurveStorage, self).__init__()
        if not self.is_config_changed(endpoint, access_key, secret_key, secure, bucket):
            return
        self._access_key = access_key
        self._secret_key = secret_key
        self._secure = secure
        self._url = endpoint
        self._bucket = bucket
        self._fileName = ""  # type: str
        self._headersMap = {
            "cur_w": u'角度',
            "cur_m": u'扭矩',
            "cur_t": u'时间',
            "cur_s": u'转速'
        }
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
    def ObjectName(self):
        entity_id = self.entity_id
        self._fileName = self.get_file_name(entity_id)
        return self._fileName

    @staticmethod
    def get_file_name(entity_id: Optional[str]) -> str:
        if entity_id:
            _fileName = "{}.csv".format(entity_id)
        else:
            _fileName = "{}.csv".format(uuid.uuid4())
        return _fileName

    @property
    def endpoint(self):
        return self._url

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
                             )  # 线程安全，每个进程需要一个实例

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

    def convertCSVData(self, curve: Dict):
        data = Dataset()
        headers = []
        for k, v in curve.items():
            try:
                header = self._headersMap[k]
                col = tuple(v if isinstance(v, list) else [])
                data.append_col(col, header=header)
                headers.append(header)
            except Exception as e:
                _logger.warn(repr(e))
                continue
        data.headers = headers
        return data.export('csv').encode('utf-8')

    def ensure_connect(self):
        if not self._client:
            self.connect()

    def remove_curves(self, curve_files: Optional[List] = None) -> bool:
        ret = False
        if not curve_files:
            return ret
        try:
            self.ensure_bucket(self._bucket)
            ret = self._client.remove_objects(self._bucket, curve_files)
            if not ret:
                raise Exception('Remove Object: {} Error'.format(','.join(curve_files)))
        except Exception as e:
            raise e
        return ret

    def write_curve(self, data: Optional[Dict] = None) -> None:
        if not data:
            raise Exception(u"未传入数据!")
        curve = data.get('curve', None)
        if not curve:
            raise Exception(u"未传入曲线!")
        try:
            # if not self._client:
            #     raise Exception(u'OSS客户端未创建!!!')

            # self._client 在 self.ensure_bucket 中保证
            self.ensure_bucket(self._bucket)
            data = self.convertCSVData(curve)
            f = io.BytesIO(data)  # 必须转换成rawIO数据
            self._client.put_object(
                self._bucket, self.ObjectName, f, length=len(data))

        except Exception as err:
            raise Exception(u"写入曲线失败: {}".format(repr(err)))

    def csv_data_to_dict(self, data):
        f = io.StringIO(data)
        ret = {
            'cur_w': [],
            'cur_m': [],
            'cur_t': [],
            'cur_s': []
        }
        headers = f.readline().split('\r\n')[0].split(',')
        positions = []
        for key in self._headersMap.keys():
            if self._headersMap[key] not in headers:
                continue
            pos = headers.index(self._headersMap[key])
            positions.append({
                'key': key,
                'pos': pos
            })
        for row in f.readlines():
            row_data = row.split('\r\n')[0].split(',')
            for p in positions:
                ret[p['key']].append(float(row_data[p['pos']]))
        return ret

    def query_curve(self):
        self.ensure_bucket(self._bucket)
        resp = self._client.get_object(self._bucket, self.ObjectName)
        csv_data = resp.data.decode('utf-8')

        dict_data = self.csv_data_to_dict(csv_data)
        return dict_data

    def fetch_obj_via_entity_id(self, entity_id: str):
        filename = self.get_file_name(entity_id)
        resp = self._client.get_object(self._bucket, filename)
        csv_data = resp.data.decode('utf-8')

        dict_data = self.csv_data_to_dict(csv_data)
        return dict_data

    def query_curves(self, entity_ids: List[str]) -> List[Dict]:
        self.ensure_bucket(self._bucket)
        datas = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(self.fetch_obj_via_entity_id, entity_id): entity_id for entity_id in
                             entity_ids}
            for future in concurrent.futures.as_completed(future_to_url):
                entity_id = future_to_url[future]
                try:
                    data = future.result()
                    datas.append({'entity_id': entity_id, 'curve': data})
                except Exception as exc:
                    _logger.error('%r generated an exception: %s' % (entity_id, exc))
                    datas.append({'entity_id': entity_id, 'curve': []})
        return datas
