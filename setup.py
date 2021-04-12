"""Setup script for the gotoalert package."""
import glob

from setuptools import setup

PACKAGES = ['gotoalert']

REQUIRES = ['numpy',
            'astropy',
            'astroplan',
            'astroquery',
            'voevent-parse',
            'voeventdb.remote',
            'pandas',
            'requests',
            'setuptools',
            ]

# Get the version string
__version__ = None
with open('gotoalert/version.py') as f:
    exec(f.read())  # Should set __version__

setup(name='gotoalert',
      version=__version__,
      description='GOTO Alert manager',
      url='http://github.com/GOTO/goto-alert',
      author='Martin Dyer, Alex Obradovic',
      author_email='martin.dyer@sheffield.ac.uk',
      install_requires=REQUIRES,
      packages=PACKAGES,
      package_data={'': ['data/*', 'data/tests/*']},
      include_package_data=True,
      scripts=glob.glob('scripts/*'),
      zip_safe=False,
      )
