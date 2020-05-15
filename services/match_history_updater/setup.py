# -*- coding: utf-8 -*-

"""Match History Updater Module.

Pull match lists for each player that has un-updated matches played.

:copyright: (c) 2020, see LICENSE
:license: Apache 2.0, see LICENSE
"""

import os

from setuptools import setup, find_packages


def open_local(filename):  # pragma: no cover
    """Open a file in this directory."""
    heredir = os.path.abspath("")
    return open(os.path.join(heredir, filename), 'r')


def read_requires(filename):  # pragma: no cover
    """Read installation requirements from pip install files."""
    with open_local(filename) as reqfile:
        lines = [line.strip() for line in reqfile.readlines()]
    return lines


if __name__ == "__main__":  # pragma: no cover
    install_requires = read_requires('requirements.txt')
    setup(
        name="Match History Updater Module",
        description="Uses the Match Endpoint to pull match-lists "
                    "for each player.",
        version='0.0.1',
        packages=find_packages(),
        install_requires=install_requires
        )
