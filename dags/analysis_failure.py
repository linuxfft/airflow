# -*- coding:utf-8 -*-
import datetime as dt
import json
import os
from airflow.models import DAG, DagRun
import pendulum
from airflow.operators.python import PythonOperator
from plugins.utils.logger import generate_logger

_logger = generate_logger(__name__)

analysis_failure_handler_concurrency = int(os.getenv('ANALYSIS_FAILURE_HANDLER_CONCURRENCY', '16'))

handler_dag = DAG(
    dag_id='analysis_failure_handler',
    description=u'处理分析异常',
    schedule_interval=None,
    default_args={
        'owner': 'qcos',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    concurrency=analysis_failure_handler_concurrency,
    max_active_runs=analysis_failure_handler_concurrency,
    tags=['analyze']
)


def handle_analysis_failure(dag_run):
    """
    处理分析异常
    @param dag_run:
    @return:
    """
    if isinstance(dag_run, DagRun):
        params = getattr(dag_run, 'conf')
    elif isinstance(dag_run, dict):
        params = dag_run.get('conf', None)
    else:
        raise Exception('无法解析触发参数')
    # 获取失败结果的entity_id
    _logger.debug(f'接收到分析异常，params:{json.dumps(params)}')
    entity_id = params.get('entity_id', None)
    # 尝试重新触发分析
    from plugins.trigger_analyze.trigger_analyze_plugin import TriggerAnalyzeHook
    TriggerAnalyzeHook.trigger_analyze_with_entity_id(entity_id)


handler_task = PythonOperator(
    provide_context=True,
    task_id='analysis_failure_handler_task',
    dag=handler_dag,
    python_callable=handle_analysis_failure
)
