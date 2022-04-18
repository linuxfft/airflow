# -*- coding:utf-8 -*-
import csv


def load_data_from_csv(file_name: str, data_keys=None) -> list:
    '''
    从csv文件中加载数据
    @params
    file_name: csv文件名
    data_keys: 将csv中的列名转换为返回字典数组中的键名
    例如： 传入 {'key_name':'col_name'} 将会读取csv文件中的‘col_name’列，并返回: [{'key_name': value}, ...]
    '''
    with open(file_name, newline='') as file:
        reader = csv.DictReader(file)
        data = []
        if not data_keys:
            for row in reader:
                data.append(dict(row))
            return data
        for row in reader:
            row_data = {}
            for k, v in data_keys.items():
                row_data.update({
                    k: row[v]
                })
            data.append(row_data)
        return data
