#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

from setuptools import setup, find_packages


def open_local(filename):
    """Open a file in this directory."""
    heredir = os.path.abspath(".")
    return open(os.path.join(heredir, filename), 'r')


def read_requires(filename):
    """Read installation requirements from pip install files."""
    NO_JENKINS = {'psycopg2-binary'}
    NO_WINDOWS = {'uvloop'}
    with open_local(filename) as reqfile:
        lines = [line.strip() for line in reqfile.readlines()]
    if os.environ.get('USER') == 'jenkins':
        lines = [line for line in lines if line.lower() not in NO_JENKINS]
    if "win" in sys.platform:
        lines = [line for line in lines if line.lower() not in NO_WINDOWS]
    return lines


if __name__ == '__main__':

    install_requires = read_requires('requirements.txt')
    setup(
        name='Lightshield',
        description='Automatic service structure to keep up to date on the Riot Api.',
        version='1.0.0',
        packages=find_packages(),
        install_requires=install_requires,
        license='APL2',
        maintainer='Robin Holzwarth',
        maintainer_email='robinholzwarth95@gmail.com',
        keywords='riot games api',
        classifiers=[
            'Development Status :: 4 - Beta',
            'Environment :: Other Environment',
            'Programming Language :: Python :: 3.8'
        ]

    )