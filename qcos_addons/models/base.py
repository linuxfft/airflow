"""
此插件的model继承此Base类，创建表时只创建此插件的表
"""
from sqlalchemy.ext.declarative import as_declarative


@as_declarative(name="Base")
class Base(object):
    '''
    数据模型的基类
    '''
    __table_args__ = {"extend_existing": True}

    @classmethod
    def create_model(cls, engine):
        if not engine.dialect.has_table(engine, cls.__tablename__):
            Base.metadata.create_all(engine)
