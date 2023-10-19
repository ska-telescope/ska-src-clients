#!/usr/bin/env python

import glob

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('VERSION') as f:
    version = f.read()

data_files = [
    ('etc', ['etc/srcnet-clients-config.yml']),
]
scripts = glob.glob('bin/*')

setup(
    name='ska_src_clients',
    version=version,
    description='Clients for SRCNet.',
    url='',
    author='rob barnsley',
    author_email='rob.barnsley@skao.int',
    packages=['ska_src_clients.client', 'ska_src_clients.common', 'ska_src_clients.session'],
    package_dir={'': 'src'},
    data_files=data_files,
    scripts=scripts,
    include_package_data=True,
    install_requires=requirements,
    classifiers=[]
)
