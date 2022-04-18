import json
from typing import Callable, TypeVar, cast

from .constants import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
from datetime import datetime
import logging
from airflow.settings import TIMEZONE
from airflow.utils.session import create_session
import functools

T = TypeVar("T", bound=Callable)

_logger = logging.getLogger(__name__)


def access_log(event, page, msg):
    '''
    一个装饰器，用于将访问事件记录日志，供抓取分析，格式和参数对照见constants.py文件
    示例：
    @access_log('VIEW', 'CURVES', '查看曲线对比页面')
    '''
    def decorator(func: T) -> T:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _logger.info(repr(args))
            _logger.info(repr(kwargs))

            ret = func(*args, **kwargs)
            from flask_login import current_user  # noqa: F401
            full_msg = CUSTOM_LOG_FORMAT.format(
                datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
                current_user if current_user and current_user.is_active else 'anonymous',
                getattr(current_user, 'last_name', '') if current_user and current_user.is_active else 'anonymous',
                CUSTOM_EVENT_NAME_MAP[event], CUSTOM_PAGE_NAME_MAP[page], msg
            )
            _logger.info(full_msg)
            return ret

        return cast(T, wrapper)

    return decorator
