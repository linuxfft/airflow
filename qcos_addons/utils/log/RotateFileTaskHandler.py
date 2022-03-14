from airflow.utils.log.file_task_handler import FileTaskHandler
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import TaskInstance


class RotateFileTaskHandler(FileTaskHandler):
    def __init__(self, base_log_folder: str, filename_template: str, maxBytes=0, backupCount=0):
        FileTaskHandler.__init__(self, base_log_folder, filename_template)
        self.maxBytes = maxBytes
        self.backupCount = backupCount

    def set_context(self, ti: "TaskInstance"):
        """
        Provide task_instance context to airflow task handler.

        :param ti: task instance object
        """
        local_loc = self._init_file(ti)
        self.handler = RotatingFileHandler(
            local_loc, encoding='utf-8', maxBytes=self.maxBytes,
            backupCount=self.backupCount
        )
        if self.formatter:
            self.handler.setFormatter(self.formatter)
        self.handler.setLevel(self.level)
