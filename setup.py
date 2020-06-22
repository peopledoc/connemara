from setuptools import setup, find_packages

setup(
    name='connemara',
    version='1.0.0',
    description='Script used for performing logical basebackups',
    package_dir={'': 'connemara_python'},
    packages=['connemara', 'connemara.sqlparser'],
    scripts=['connemara_python/bin/connemara_basebackup.py'],
    install_requires=['psycopg2', 'pglast']
)
