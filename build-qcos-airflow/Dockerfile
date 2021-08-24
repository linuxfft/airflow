FROM docker.pkg.github.com/masami10/airflow/airflow-base:latest

COPY requirements-airflow-prod.txt /tmp/requirements.txt

COPY dags/ /default/dags/

COPY plugins/ /default/plugins/

ENV RUNTIME_ENV prod

USER root

RUN set -ex \
    && chown -R airflow:airflow /usr/local/airflow/plugins/ \
    && chown -R airflow:airflow /usr/local/airflow/dags/

USER airflow

RUN set -ex \
    && ls -al  /usr/local/airflow/plugins/ \
    && sed -i 's/rbac = False/rbac = True/g' ${AIRFLOW_DEFAULT}/airflow.cfg \
    && cat ${AIRFLOW_DEFAULT}/airflow.cfg \
    && pip install -r /tmp/requirements.txt --user -i https://pypi.douban.com/simple
