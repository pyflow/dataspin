
from .function import *


function_class_map = {
    'splitby': SplitByFunction,
    'save': SaveFunction
}

def get_function_class(type: str, conf):
    function_class = function_class_map.get(type, Function)
    return function_class(conf)