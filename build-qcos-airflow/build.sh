#!/usr/bin/env bash
cp -r ../dags ./dags
cp -r ../plugins ./plugins
cp -r ../qcos_addons ./qcos_addons
docker build -t registry.centron.cn:5000/airflow/airflow:latest --no-cache .
