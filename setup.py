"""Setup script for the gtecs-alert package."""
import glob

from setuptools import setup, find_namespace_packages

REQUIRES = ['numpy',
            'astropy',
            'astroplan',
            'astroquery',
            'pygcn>=1.1.1',
            'hop-client>=0.9.0',
            'voeventdb.remote',
            'pandas',
            'requests',
            'setuptools',
            ]

setup(name='gtecs-alert',
      version='0',
      description='G-TeCS functions for handling transient alerts',
      url='http://github.com/GOTO/goto-alert',
      author='Martin Dyer',
      author_email='martin.dyer@sheffield.ac.uk',
      install_requires=REQUIRES,
      packages=find_namespace_packages(include=['gtecs*']),
      package_data={'gtecs': ['alert/data/*', 'alert/data/test_notices/*']},
      scripts=glob.glob('scripts/*'),
      zip_safe=False,
      )
