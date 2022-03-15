import json
import zipfile

from jinja2.utils import htmlsafe_json_dumps  # type: ignore

from airflow.configuration import AIRFLOW_HOME
from plugins.utils.utils import get_result, get_curves
import os
import pandas as pd
from airflow.models import Variable
from typing import List
from pathlib import Path
import logging
from qcos_addons.models.result import ResultModel

_logger = logging.getLogger(__name__)


class CurveResultDownloader:
    """
    用于批量下载曲线和结果
    """

    def __init__(self):
        pass

    _cache_folder = os.path.join(AIRFLOW_HOME, 'downloads/contents')
    _max_download_count = os.environ.get('ENV_CURVE_MAX_DOWNLOAD_COUNT', 1000)

    @classmethod
    def cache_folder(cls):
        """
        获取下载缓存目录
        """
        if not os.path.exists(cls._cache_folder):
            os.makedirs(cls._cache_folder, exist_ok=True)
        return cls._cache_folder

    @classmethod
    def cache_contents(cls, entity_ids: List[str] = None, results: List[ResultModel] = None) -> List[str]:
        '''
        在服务器上缓存需要下载的曲线和结果
        '''
        files = []
        base_path = cls.cache_folder()
        result_table = pd.DataFrame()
        result = None
        if results is None and entity_ids is None:
            raise Exception('未选中结果或曲线')
        if results is not None:
            result_table = pd.DataFrame(list(map(lambda r: r.as_dict(), results)))
            result = results[0].as_dict()
            entity_ids = list(map(lambda r: r.entity_id, results))
        else:
            for entity_id in entity_ids:
                try:
                    result = get_result(entity_id)
                    tb = pd.DataFrame(result, index=[0])
                    result_table = pd.concat([result_table, tb], ignore_index=True)
                except Exception as e:
                    _logger.error(e)

        if result is not None and result.get('device_type') == 'servo_press':
            result_keys_translation_mapping = Variable.get(
                'servo_press_result_keys_translation_mapping',
                deserialize_json=True,
                default_var={}
            )
        else:
            result_keys_translation_mapping = Variable.get(
                'result_keys_translation_mapping',
                deserialize_json=True,
                default_var={}
            )
        result_table.rename(columns=lambda x: result_keys_translation_mapping.get(x, x), inplace=True)
        try:
            curves = get_curves(entity_ids)
            for curve in curves:
                entity_id = curve.get('entity_id')
                f = f'{entity_id}.csv'.replace('/', '@')
                f = os.path.join(base_path, f)
                dd = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in curve.get('curve').items()]))
                dd.to_csv(f, index=False)
                files.append(f)
        except Exception as e:
            _logger.error(e)
        try:
            rf = os.path.join(base_path, "results.csv")
            result_table.to_csv(rf, index=False)
            files.append(rf)
        except Exception as e:
            _logger.error(e)
        return files

    @classmethod
    def clean_cached_files(cls):
        """
        清除缓存的文件
        """
        fds = ['*.json', '*.csv']
        folder = cls.cache_folder()
        for fd in fds:
            for f in Path(folder).glob(fd):
                try:
                    f.unlink()
                except OSError as e:
                    _logger.error(f"Error: {f} : {e}")

    @classmethod
    def prepare_download_file(cls, entity_ids: List[str] = None, results: List[ResultModel] = None):
        """
        创建需要下载的文件并返回文件名
        """
        fn = f'{cls.cache_folder()}/curves.zip'
        chk_file = Path(fn)

        if chk_file.is_file():
            chk_file.unlink()

        if entity_ids is None and results is None:
            raise Exception('未选中结果或曲线')
        n = len(results) if results else len(entity_ids)
        if n > cls._max_download_count:
            raise Exception(f'单次下载最大支持{cls._max_download_count}条曲线，当前为{n}条')
        files = cls.cache_contents(entity_ids=entity_ids, results=results)

        if not files:
            raise Exception('未生成数据')
        with zipfile.ZipFile(fn, 'w') as f:
            for file in files:
                if not os.path.exists(file):
                    continue
                f.write(file, arcname=os.path.basename(file), compress_type=zipfile.ZIP_DEFLATED)
        cls.clean_cached_files()
        return fn
