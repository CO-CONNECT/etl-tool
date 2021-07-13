import coconnect
import os
import sys
import json
import glob
import inspect

from . import extract
from .dag import make_dag

from .file_helpers import (
    load_json,
    load_csv,
    get_file_map_from_dir,
    get_mapped_fields_from_rules
)

from .rules_helpers import(
    get_source_field,
    get_source_table,
    apply_rules
)

_DEBUG = False

def set_debug(value):
    global _DEBUG
    _DEBUG = value
    
def get_classes(format=False):

    if config_folder:= os.environ.get('COCONNECT_CONFIG_FOLDER'):
        sys.path.append(config_folder)
        files = [x for x in os.listdir(config_folder) if x.endswith(".py") and not x.startswith('__')]
        retval = {}
        for fname in files:
            print (fname)
            mname = fname.split(".")[0]
            print (mname)
            exit(0)
            continue
            module = __import__(mname,fromlist=[fname])
            path = os.path.join(config_folder,fname)
            defined_classes = {
                m[0]: {
                    'module':m[1].__module__,
                    'path': path  if not os.path.islink(path) else os.readlink(path),
                    'sympath': path,
                    'last-modified': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(os.path.join(config_folder,fname))))
                }
                for m in inspect.getmembers(module, inspect.isclass)
                if m[1].__module__ == module.__name__
            }
            print (defined_classes)
        exit(0)
        
    return get_classes_from_tool(format=format)
    
def get_classes_from_tool(format=format):
    import time
    from coconnect.cdm import classes
    _dir = os.path.dirname(classes.__file__)
    files = [x for x in os.listdir(_dir) if x.endswith(".py") and not x.startswith('__')]
    retval = {}
    for fname in files:
        mname = fname.split(".")[0]
        mname = '.'.join([classes.__name__, mname])
        module = __import__(mname,fromlist=[fname])
        path = os.path.join(_dir,fname)
        defined_classes = {
            m[0]: {
                'module':m[1].__module__,
                'path': path  if not os.path.islink(path) else os.readlink(path),
                'sympath': path,
                'last-modified': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(os.path.join(_dir,fname))))
            }
            for m in inspect.getmembers(module, inspect.isclass)
            if m[1].__module__ == module.__name__
        }
        retval.update(defined_classes)
    if format:
        return json.dumps(retval,indent=6)
    else:
        return retval

