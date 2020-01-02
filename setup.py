from setuptools import setup

setup(
   author="Sietse Snel",
   author_email="s.t.snel@uu.nl",
   description=('Toolkit to detect issues in iRODS ICAT-database'),
   install_requires=[
       'psycopg2-binary>=2.7.7',
   ],
   name='icat_tools',
   packages=['icat_tools','icat_tools.detectors'],
   entry_points={
       'console_scripts': [
           'icat-database-checker = icat_tools.dbcheck_command:entry'
       ]
   },
   version='0.0.1',
)
