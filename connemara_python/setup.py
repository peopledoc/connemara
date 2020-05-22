from setuptools import setup, find_packages

setup(
    name='connemara_basebackup',
    version='1.0.0',
    description='Script used for performing logical basebackups',
    packages=find_packages(),
    scripts=['bin/connemara_basebackup.py'],
    install_requires=['psycopg2', 'pglast']
)
