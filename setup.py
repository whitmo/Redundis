from setuptools import find_packages
from setuptools import setup

version = '0.0'

requires=['redis',
          'requests',
          'path.py',
          'pyyaml',
          'stuf'],

setup(name='Redundis',
      version=version,
      description="",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='redis nosql failover',
      author='whit',
      author_email='whit at surveymonkey.com',
      url='http://redundis.github.com',
      license='BSD',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      entry_points="""
      [console_scripts]
      dundis = redundis.cli:main

      [redundis.cli]
      devinst = redundis.devinst:DevInstall
      """,
      )
