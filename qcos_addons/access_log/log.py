from .constants import CUSTOM_LOG_FORMAT, CUSTOM_EVENT_NAME_MAP, CUSTOM_PAGE_NAME_MAP
from datetime import datetime
import logging
from flask_login import current_user  # noqa: F401

_logger = logging.getLogger(__name__)


def access_log(event, page, msg):
    def decorator(func, *args, **kwargs):
        msg = CUSTOM_LOG_FORMAT.format(
            datetime.now(tz=TIMEZONE).strftime("%Y-%m-%d %H:%M:%S"),
            current_user if current_user and current_user.is_active else 'anonymous',
            getattr(current_user, 'last_name', '') if current_user and current_user.is_active else 'anonymous',
            CUSTOM_EVENT_NAME_MAP[event], CUSTOM_PAGE_NAME_MAP[page], msg
        )
        _logger.info(msg)
        return func(*args, **kwargs)

    return decorator
