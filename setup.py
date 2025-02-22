#!/usr/bin/env python3
from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r') as f:
        return f.readlines()

setup(
    name="rafdb",
    version="1.0",
    packages=find_packages(exclude=('tests', 'tests.*')),
    package_data={
        'rafdb': ['csv_data/*'],
    }
)
