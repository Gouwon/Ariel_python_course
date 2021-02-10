#-*- coding: utf-8 -*-

import os
import json
import ruamel.yaml
from pprint import pprint


def join_path(*paths):
    """
    문자열로 넘어온 패스를 합쳐서 반환합니다.
    """
    basedirectory = os.path.dirname(__file__)
    result__filepath = basedirectory

    for path in paths:
        result__filepath = os.path.join(result__filepath, path)
    
    return result__filepath


def convert_yaml_into_dict(filepath: str) -> dict:
    """
    filepath에 해당 파일이 있는지 확인하여 있으면, dictionary를 반환합니다.
    """
    if os.path.isfile(filepath):
        with open(filepath, 'r') as f:
            ordered_dict = ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader)
            result_dict = json.loads(json.dumps(ordered_dict))
        
        pprint(result_dict, indent=4)
        return result_dict

    return None

