"""
A command line interface
"""
import argparse
from .setup_env import Setup
import sys


def main(argv=None):
    if argv is None: #pragma: no cover
        argv = sys.argv[1:] 
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='commands')
    
    setup_parser = subparsers.add_parser('setup', help='Create a cluster')

    setup_parser.add_argument('-c', '--config', action='store',
                                default='egg:FlailDis#flaildis/etc/default_setup.yml',
                                help='config file for specifying cluster characteristics')
    setup_parser.add_argument('spec', action='store',
                                default='default',
                                help='which cluster specification to install')

    ## setup_parser.add_argument('-r', '--read-only', 
    ##                           default=False,
    ##                           action='store_true',
    ##                           help='use git readonly dependencies')

    setup_parser.set_defaults(func=Setup.run)
    args = parser.parse_args(args=argv)
    return args.func(args)
