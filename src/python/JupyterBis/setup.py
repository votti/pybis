import os

from setuptools import setup

setup(name='jupyterbis',
      version='0.1.0',
      description='A package that allows integration between Jupyter and openBIS.',
      url='https://sissource.ethz.ch/sis/pybis/',
      author='SIS | ID |ETH Zuerich',
      author_email='chandrasekhar.ramakrishnan@id.ethz.ch',
      license='BSD',
      packages=['jupyterbis'],
      install_requires=[
          'pytest',
          'jupyterhub',
          'pybis'
      ],
      zip_safe=True)
