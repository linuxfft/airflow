import json
import os

# 默认的设备view_cofnig, 单条曲线分析页面的显示内容控制，默认值不建议修改，可以在设备类型模型view_cofnig字段中为不同设备类型单独配置
ENV_DEFAULT_DEVICE_VIEW_CONFIG = os.environ.get('ENV_DEFAULT_DEVICE_VIEW_CONFIG', json.dumps({
    "curve_key_map": {
        "cur_w": "角度",
        "cur_m": "扭矩",
        "cur_t": "时间",
        "cur_s": "转速"
    }, "curve_unit_map": {"cur_m": "牛·米", "cur_w": "度", "cur_t": "秒", "cur_s": "转/分"},
    "display_keys": {"basic": ["entity_id", "bolt_number", "update_time", "controller_name", "tool_sn"],
                     "detail": ["vin", "measure_result", "measure_torque", "torque_target", "torque_max",
                                "torque_min", "measure_angle", "angle_target", "angle_max", "angle_min",
                                "strategy", "tightening_id", "job", "batch_count", "pset", "channel_id",
                                "error_code"]},
    "translation_mapping": {"angle_max": "最大角度", "angle_min": "最小角度", "angle_target": "目标角度", "batch": "批次",
                            "batch_count": "批次号", "channel_id": "通道号", "controller_name": "控制器名称",
                            "controller_sn": "控制器序列号", "count": "拧紧次数", "error_code": "错误信息",
                            "group_seq": "组序号", "job": "JOB", "measure_angle": "测量角度",
                            "measure_result": "拧紧结果", "measure_time": "测量时间", "measure_torque": "测量扭矩",
                            "nut_no": "螺栓编号", "pset": "PSET", "seq": "拧紧点序号", "strategy": "策略",
                            "tightening_id": "拧紧ID", "tool_sn": "工具序列号", "torque_max": "最大扭矩",
                            "torque_min": "最小扭矩", "torque_target": "目标扭矩", "torque_threshold": "扭矩阈值",
                            "update_time": "更新时间", "user_id": "用户ID", "workorder_id": "工单号"}
}))
