#-*-coding:utf-8-*-

from os.path import dirname, join
import io, re
# from pip.req import parse_requirements

from setuptools import (
    find_packages,
    setup,
)


def parse_requirements(filename):
    """ load requirements from a pip requirements file """
    lineiter = (line.strip() for line in open(filename))
    return [line for line in lineiter if line and not line.startswith("#")]


with io.open('QuadQuanta/__init__.py', 'rt', encoding='utf8') as f:
    context = f.read()
    VERSION = re.search(r'__version__ = \'(.*?)\'', context).group(1)
    AUTHOR = re.search(r'__author__ = \'(.*?)\'', context).group(1)

setup(
    name='QuadQuanta',  # 模块名称
    version=VERSION,
    description='To build a quantification system',  # 描述
    packages=find_packages(exclude=[]),
    author='AUTHOR',
    author_email='',
    license='MIT',
    package_data={'': ['*.*']},
    url='#',
    include_package_data=True,
    install_requires=parse_requirements("requirements.txt"),  # 所需的运行环境
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