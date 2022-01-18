# -*- coding:utf-8 -*-
import json


def load_data_from_json(file_name: str) -> dict:
    f = open(file_name)
    return json.load(f)
