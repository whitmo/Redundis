#from .resolver import DottedNameResolver
from collections import OrderedDict
from path import path
import json
import pkg_resources
import yaml


try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


def yml_load(stream, loader=Loader):
    return yaml.load_all(stream, loader)


#resolve = DottedNameResolver(None).maybe_resolve
    


def resource_spec(spec):
    """
    Loads resource from a string specifier. 
    
    >>> config.resource_spec('egg:monkeylib#data/languages.ini')
    '.../monkeylib/data/languages.ini'

    >>> config.resource_spec('file:data/languages.ini')
    'data/languages.ini'

    >>> config.resource_spec('data/languages.ini')
    'data/languages.ini'
    """
    filepath = spec
    if spec.startswith('egg:'):
        req, subpath = spec.split('egg:')[1].split('#')
        req = pkg_resources.Requirement.parse(req)
        filepath = res_filename(req, subpath)
    elif spec.startswith('file:'):
        filepath = spec.split('file:')[1]
    # Other specs could be added, but egg and file should be fine for
    # now
    return filepath


def res_stream(req, path):
    return pkg_resources.resource_stream(req, path)


def res_filename(req, path):
    return pkg_resources.resource_filename(req, path)


def res_json(req, path):
    return json.load(res_stream(req, path))


def redis_conf_to_dict(fp):
    fp = path(fp)
    tuples = (x.split(' ', 1) for x in path(fp).text().split('\n') 
              if x and not x.startswith('#'))
    return OrderedDict(tuples)
    
def od_to_redis_conf(od, outfile):
    path(outfile).write_text('\n'.join("%s %s" %(x, y) for x, y in od.items()))
    return path(outfile).text()
