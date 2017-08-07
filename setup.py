from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
#     long_description = f.read()

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requires = f.read().split('\n')

setup(
    name='celery_deadline',
    version='0.0.1',
    description='A sample Python project',
    # long_description=long_description,
    url='https://github.com/chadrik/celery-deadline',
    author='Chad Dombrova',
    author_email='chadrik@gmail.com',
    license='MIT',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        # 'Programming Language :: Python :: 3',
        # 'Programming Language :: Python :: 3.3',
        # 'Programming Language :: Python :: 3.4',
        # 'Programming Language :: Python :: 3.5',
    ],

    # keywords='sample setuptools development',

    # packages=find_packages(exclude=['contrib', 'docs', 'tests']),


    py_modules=["celery_deadline"],
    install_requires=requires,

    # # List additional groups of dependencies here (e.g. development
    # # dependencies). You can install these using the following syntax,
    # # for example:
    # # $ pip install -e .[dev,test]
    # extras_require={
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },
)
