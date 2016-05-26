import os

from setuptools import setup

setup(name='pybis',
      version='0.1.0',
      description='A package for interacting with openBIS.',
      url='https://sissource.ethz.ch/sis/pybis/',
      author='SIS | ID |ETH Zuerich',
      author_email='chandrasekhar.ramakrishnan@id.ethz.ch',
      license='BSD',
      packages=['pybis'],
      install_requires=[
          'pytest',
          'click'
      ],
      entry_points='''
        [console_scripts]
        pybis=pybis.scripts.cli:main
      ''',
      zip_safe=True)
