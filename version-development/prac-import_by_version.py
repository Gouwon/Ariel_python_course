#-*- coding: utf-8 -*-

def import_module_v3():
    '''
    모듈 다시불러오는 함수. 파이썬 인터프리터 버전에 따라서 reload() built-in 함수를 호출하는 방법이 다름.
    python3 에서는 imp 모듈의 reload()를 호출해야 하며, python2에서는 reload()를 바로 호출할 수 있음.
    반환하는 결과는 imp 모듈의 reload함수 자체이고, 이를 버전에 따라서 필요시에 변수에 담아서 사용하면 됨.
    '''
    import sys
    import re

    _p = '(?P<version>[\d|.]+)\s'
    _interpreter_info = sys.version
    _interpreter_version = re.search(_p, _interpreter_info).group('version')

    _i_v = int(_interpreter_info.split('.')[0])
    print('PYTHON VERSION IS {}'.format(_i_v))

    if _i_v != 2:
        import importlib
        tmp = importlib.import_module('imp')

        return tmp.reload

if import_module_v3():
    reload = import_module_v3()


def test_module_import():
    import tlib
    ## ./tlib.py > RES_SC = 'SUCCESS'
    reload(tlib)
    print(tlib.RES_SC)
    return tlib.RES_SC

test_module_import()