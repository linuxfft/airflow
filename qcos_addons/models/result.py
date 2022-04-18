from sqlalchemy.orm import relationship
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils import timezone
from sqlalchemy import Boolean, Float, Text
from airflow.utils.db import provide_session
from sqlalchemy import Column, String, Integer
from qcos_addons.models.base import Base
from distutils.util import strtobool
from sqlalchemy import text, ForeignKey, sql
import os
import json

ENV_TIMESCALE_ENABLE = strtobool(os.environ.get('ENV_TIMESCALE_ENABLE', 'false'))


class ResultModel(Base):
    """
    result: 拧紧结果数据模型
    """

    def __repr__(self):
        return self.entity_id

    def __init__(self, *args, **kwargs):
        self.update_time = timezone.utcnow()
        super(ResultModel, self).__init__(*args, **kwargs)

    __tablename__ = "result"

    pk = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(Integer)  # rush id
    entity_id = Column(String(256), unique=True)
    tool_sn = Column(String(256))
    angle_max = Column(Float)
    angle_min = Column(Float)
    angle_target = Column(Float)
    batch = Column(String(32))
    batch_count = Column(Integer)
    channel_id = Column(Integer)
    controller_name = Column(String(256))
    controller_sn = Column(String(256))
    count = Column(Integer)
    device_type = Column(String(32))
    error_code = Column(String(64))
    group_seq = Column(Integer)
    job = Column(Integer)
    measure_angle = Column(Float)
    measure_result = Column(String(32))
    measure_time = Column(Float)
    measure_torque = Column(Float)
    nut_no = Column(String(256))
    pset = Column(Integer)
    seq = Column(Integer)
    step_results = Column(Text)
    strategy = Column(String(16))
    tightening_id = Column(String(128))
    torque_max = Column(Float)
    torque_min = Column(Float)
    torque_target = Column(Float)
    torque_threshold = Column(Float)
    update_time = Column(UtcDateTime())
    user_id = Column(Integer)
    workorder_id = Column(Integer)
    vin = Column(String(256))
    task_id = Column(String(250))  # 分析任务ID
    dag_id = Column(String(250))  # 分析任务DAG ID
    execution_date = Column(UtcDateTime)  # 分析任务执行时间
    line_code = Column(String(100))  # 产线代码
    factory_code = Column(String(100))  # 工厂代码
    error_tag = Column(String(1000))  # 曲线分析异常标签
    result = Column(String(20))  # 分析结果, OK/NOK
    verify_error = Column(Integer)  # 曲线分析返回值
    final_state = Column(String(20))  # 最终状态牵涉2次检验
    # controller_name = Column(String(100))  # 控制器名称@工位编号/工位名称
    bolt_number = Column(String(1000))  # 螺栓编号 {controller_name}_{job}_{batch_count}_{pset}
    craft_type = Column(Integer)  # 工艺类型
    car_code = Column(String(1000))  # 车辆编号
    type = Column(String(100), default="normal")  # 任务实例类型，normal/rework,正常/返修
    should_analyze = Column(Boolean(), default=True)  # 是否需要分析
    training_task_id = Column(String(250))  # 训练任务ID
    training_dag_id = Column(String(250))  # 训练DAG ID
    training_execution_date = Column(UtcDateTime)  # 训练任务执行时间
    controller_id = Column(Integer,
                           ForeignKey('tightening_controller.id', onupdate='CASCADE', ondelete='RESTRICT'),
                           nullable=True, default=sql.null())
    controller = relationship("TighteningController", foreign_keys=[controller_id], lazy='joined')

    def as_dict(self):
        v: dict = self.__dict__
        if v:
            if v.get('_sa_instance_state'):
                v.pop('_sa_instance_state')
            if v.get('step_results') and isinstance(v['step_results'], str):
                v['step_results'] = json.loads(v['step_results'])
            if v.get('controller') and not isinstance(v.get('controller'), dict):
                v['controller'] = v.get('controller').to_dict()
            return v
        else:
            return dict()

    @classmethod
    @provide_session
    def list_results(cls, craft_type=None, bolt_number=None, session=None):
        results = cls.query_results(craft_type, bolt_number, session).all()
        return results

    @classmethod
    @provide_session
    def query_results(cls, craft_type=None, bolt_number=None, session=None):
        results = session.query(cls)
        if craft_type:
            results = results.filter(cls.craft_type == craft_type)
        if bolt_number:
            results = results.filter(cls.bolt_number == bolt_number)
        return results

    @classmethod
    def filter_valid_fields(cls, data):
        result_data = {}
        for key, value in data.items():
            if hasattr(cls, key):
                result_data[key] = value
        return result_data

    @classmethod
    def create_model(cls, engine):
        if not engine.dialect.has_table(engine, ResultModel.__tablename__):
            Base.metadata.create_all(engine)
            if not ENV_TIMESCALE_ENABLE:
                return
            with engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(text(
                    f'''SELECT create_hypertable('{ResultModel.__tablename__}', 'update_time','tool_sn', 4, chunk_time_interval => INTERVAL '1 month', migrate_data => TRUE);'''))
