import glob

from setuptools import setup

setup(name='gotoalert',
      version='0.1',
      description='GOTO Alert manager',
      url='http://github.com/GOTO/goto-alert',
      author='Alex Obradovic',
      author_email='aobr10@student.monash.edu',
      packages=['gotoalert'],
      package_data={'': ['data/*']},
      install_requires=['numpy', 'astropy', 'astroplan', 'voevent-parse',
                        'slacker', 'slackclient', 'setuptools'],
      scripts=glob.glob('scripts/*'),
      include_package_data=True,
      zip_safe=False)
