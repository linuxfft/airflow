import os

factory_code_map = {
    'nd': ['nd', '7200', 'ND'],
    'lg': ['lg', '2200', 'LG'],
}


def get_factory_code():
    """
    获取工厂代码
    @return: str 工厂代码
    """
    code = os.environ.get('FACTORY_CODE', '')
    for key, values in factory_code_map.items():
        if code in values:
            return key
    return code
