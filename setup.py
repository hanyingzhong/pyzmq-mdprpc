# -*- coding: utf-8 -*-

"""Module containing client-broker-worker communication library based on ZMQ and Majordomo Protocol
"""

__license__ = """
    GNU General Public License <http://www.gnu.org/licenses/>
"""

__author__ = 'Matej Capkovic'
__email__ = 'capkovic@gmail.com'

from setuptools import setup

setup(
    name        = 'pyzmq-mdprpc',
    version     = '0.1.0',
    description = 'Remote procedure call based on ZMQ and modified Majordomo Protocol',
    author      = 'Matej Capkovic',
    author_email= 'capkovic@gmail.com',
    url         = 'https://github.com/capkovic/pyzmq-mdprpc',
    package_dir = {'mdprpc': 'mdprpc'},
    packages    = ['mdprpc'],
    install_requires = [
        'msgpack-python',
        'pyzmq >= 13',
    ],
    zip_safe    = False,
)

