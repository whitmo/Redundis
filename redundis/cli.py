"""
A command line interface
"""
from cliff.app import App
from cliff.commandmanager import CommandManager
import logging
import sys
import pkg_resources


dist = pkg_resources.get_distribution('Redundis')


class Dundis(App):

    log = logging.getLogger(__name__)

    def __init__(self):
        super(Dundis, self).__init__(
            description='Redundis command line interface',
            version=dist.version,
            command_manager=CommandManager('redundis.cli'),
            )

    def prepare_to_run_command(self, cmd):
        self.log.debug('prepare_to_run_command %s', cmd.__class__.__name__)

    def clean_up(self, cmd, result, err):
        self.log.debug('clean_up %s', cmd.__class__.__name__)
        if err:
            self.log.debug('got an error: %s', err)

    @classmethod
    def main(cls, argv=sys.argv[1:]):
        return cls().run(argv)

main = Dundis.main

if __name__ == '__main__':
    sys.exit(Dundis.main(sys.argv[1:]))





# def main(argv=None):
#     if argv is None: #pragma: no cover
#         argv = sys.argv[1:] 
#     parser = argparse.ArgumentParser()
#     subparsers = parser.add_subparsers(help='commands')
    
#     setup_parser = subparsers.add_parser('setup', help='Create a cluster')

#     setup_parser.add_argument('-c', '--config', action='store',
#                                 default='egg:FlailDis#flaildis/etc/default_setup.yml',
#                                 help='config file for specifying cluster characteristics')
#     setup_parser.add_argument('spec', action='store',
#                                 default='default',
#                                 help='which cluster specification to install')

#     ## setup_parser.add_argument('-r', '--read-only', 
#     ##                           default=False,
#     ##                           action='store_true',
#     ##                           help='use git readonly dependencies')

#     setup_parser.set_defaults(func=Setup.run)
#     args = parser.parse_args(args=argv)
#     return args.func(args)
