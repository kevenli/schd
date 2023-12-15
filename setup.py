#!/usr/bin/env python3
from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r') as f:
        return f.readlines()

setup(
    name="schd",
    version="0.0.1",
    packages=find_packages(exclude=('tests', 'tests.*')),
    install_requires=read_requirements(),
    entry_points={
        'console_scripts': [
            'schd = schd.cmds.schd:main',
        ],
    },
    include_package_data=True,
)
