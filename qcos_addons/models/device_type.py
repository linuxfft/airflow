from sqlalchemy import Column, String, Integer, Text
from qcos_addons.models.base import Base


class DeviceTypeModel(Base):
    '''
    device type: 设备类型
    '''
    __tablename__ = "device_type"

    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    name = Column(String(100), nullable=False)
    view_config = Column(Text, nullable=True)

    def __repr__(self):
        return self.name

    def __init__(self, *args, name=None, view_config=None, **kwargs):
        super(DeviceTypeModel, self).__init__(*args, **kwargs)
        self.name = name
        self.view_config = view_config
