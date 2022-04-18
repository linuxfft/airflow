from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Integer
from qcos_addons.models.base import Base


class ErrorTag(Base):
    """
    curve error tags：曲线异常标签
    """

    __tablename__ = "error_tag"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    label = Column(String(1000), nullable=False)
    value = Column(String(1000), nullable=False, unique=True)

    def __init__(
        self, *args, label=None, value=None, **kwargs):
        self.label = label
        self.value = value
        super(ErrorTag, self).__init__(*args, **kwargs)

    @staticmethod
    def _error_tag_data(error_tag):
        return error_tag.to_dict()

    def to_dict(self):
        return {
            'id': self.id,
            'label': self.label,
            'value': self.value
        }

    @staticmethod
    def _error_tag_label(error_tag):
        return error_tag.label

    @classmethod
    @provide_session
    def get_all_dict(cls, session=None):
        objs = session.query(cls).all()
        ret = {}
        for o in objs:
            ret.update({o.value: cls._error_tag_label(o)})
        return ret

    @classmethod
    @provide_session
    def get_all(cls, session=None):
        objs = session.query(cls).all()
        dataArr = list(map(cls._error_tag_data, objs))
        return dataArr
