from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Integer, Text
from qcos_addons.models.base import Base

class TighteningController(Base):
    """
    tightening controllers：拧紧控制器数据模型
    """

    __tablename__ = "tightening_controller"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    controller_name = Column(String(1000), nullable=False, unique=True)
    line_code = Column(String(1000), nullable=False)
    line_name = Column(String(1000), nullable=True)
    work_center_code = Column(String(1000), nullable=False)
    work_center_name = Column(String(1000), nullable=True)
    device_type_id = Column(Integer, ForeignKey('device_type.id', onupdate='CASCADE', ondelete='SET NULL'),
                            nullable=False)
    device_type = relationship("DeviceTypeModel", foreign_keys=[device_type_id], lazy='joined')
    config = Column(Text, nullable=True)

    field_name_map = {
        'controller_name': ['控制器名称'],
        'line_code': ['工段编号'],
        'line_name': ['工段名称'],
        'work_center_code': ['工位编号'],
        'work_center_name': ['工位名称'],
        'device_type_id': [],
    }

    def __init__(self, *args, controller_name=None, line_code=None, line_name=None, work_center_code=None,
                 work_center_name=None, device_type_id=None, **kwargs):
        super(TighteningController, self).__init__(*args, **kwargs)
        self.controller_name = controller_name
        self.line_code = line_code
        self.line_name = line_name
        self.work_center_code = work_center_code
        self.work_center_name = work_center_name
        self.device_type_id = device_type_id

    def as_dict(self):
        v: dict = self.__dict__
        if v:
            v.pop('id')
            if v.get('_sa_instance_state'):
                v.pop('_sa_instance_state')
        return v

    @classmethod
    @provide_session
    def find_controller(cls, controller_name, session=None):
        return session.query(cls).filter(cls.controller_name == controller_name).first()

    def to_dict(self):
        return {
            'id': self.id,
            'controller_name': self.controller_name,
            'line_code': self.line_code,
            'line_name': self.line_name,
            'work_center_code': self.work_center_code,
            'work_center_name': self.work_center_name,
            'device_type_id': self.device_type_id
        }

    @classmethod
    @provide_session
    def controller_exists(cls, session=None, **kwargs) -> bool:
        fields_data = cls.to_db_fields(**kwargs)
        if 'controller_name' not in fields_data:
            return False
        return cls.find_controller(
            fields_data.get('controller_name', None)
            , session=session
        ).to_dict().get('id', None) is not None

    @classmethod
    @provide_session
    def list_controllers(cls, session=None):
        controllers = list(session.query(cls).all())
        return controllers

    @classmethod
    def to_db_fields(cls, **kwargs):
        extra_fields = kwargs.keys()
        controller_data = {}
        for f in extra_fields:
            for field_name, val in cls.field_name_map.items():
                if f in val or f == field_name:
                    controller_data[field_name] = kwargs[f]
                    continue
        return controller_data

    @classmethod
    @provide_session
    def add_controller(cls, session=None, **kwargs):
        controller_data = cls.to_db_fields(**kwargs)
        session.add(TighteningController(**controller_data))

    @staticmethod
    def get_line_code_by_controller_name(controller_name):
        controller_data = TighteningController.find_controller(controller_name)
        if not controller_data:
            raise Exception('未找到控制器数据: {}'.format(controller_name))
        return controller_data.to_dict().get('line_code', None), controller_data.to_dict().get('id')
