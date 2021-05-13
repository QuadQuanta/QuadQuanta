# -*-coding:utf-8-*-

from os.path import dirname, join
import io, re
# from pip.req import parse_requirements

from setuptools import (
    find_packages,
    setup,
)

with io.open('QuadQuanta/__init__.py', 'rt', encoding='utf8') as f:
    context = f.read()
    VERSION = re.search(r'__version__ = \'(.*?)\'', context).group(1)
    AUTHOR = re.search(r'__author__ = \'(.*?)\'', context).group(1)

with open('requirements.txt') as reqs_file:
    INSTALL_REQUIRES = reqs_file.readlines()

URL = "https://github.com/levonwoo/QuadQuanta"

setup(
    name='QuadQuanta',  # 模块名称
    version=VERSION,
    description='To build a quantification system',  # 描述
    long_description='try to build a personal trading system',
    packages=find_packages(where='.', exclude=(), include=('*',)),
    author=AUTHOR,
    author_email='',
    license='MIT license',
    package_data={'': ['*.*']},
    url=URL,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)