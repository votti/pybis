import os

from setuptools import setup

setup(name='pybis',
      version='0.1.0',
      description='A package for interacting with openBIS.',
      url='https://sissource.ethz.ch/sis/pybis/',
      author='SIS | ID | ETH Zuerich',
      author_email='swen@ethz.ch',
      license='BSD',
      packages=['pybis'],
      install_requires=[
          'pytest',
          'requests',
          'datetime',
          'pandas',
          'click'
      ],
      entry_points='''
        [console_scripts]
        pybis=pybis.scripts.cli:main
      ''',
      zip_safe=True)
