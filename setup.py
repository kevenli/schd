#!/usr/bin/env python3
from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r', encoding='utf8') as f:
        return f.readlines()

setup(
    name="schd",
    version="0.0.15",
    url="https://github.com/kevenli/schd",
    packages=find_packages(exclude=('tests', 'tests.*')),
    install_requires=['apscheduler<4.0', 'pyaml', 'aiohttp'],
    entry_points={
        'console_scripts': [
            'schd = schd.cmds.schd:main',
        ],
    },
    license="ApacheV2",
    license_files="LICENSE",
)
