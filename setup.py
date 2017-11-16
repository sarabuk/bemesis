
from setuptools import setup

install_requires = [
    'pandas >= 0.14',
    'numpy >= 1.8',
    'simplejson >= 3.11',
    'paramiko >= 2.3'
]


setup(
  name = 'bemesis',
  packages = ['bemesis'], # this must be the same as the name above
  version = '1.4',
  description = 'test lib',
  author = 'bemesis team',
  author_email = 'genesis@genesis.com',
  url = 'https://github.com/sarabuk/bemesis', # use the URL to the github repo
  download_url = 'https://github.com/sarabuk/bemesis/archive/0.1.tar.gz', # I'll explain this in a second
  keywords = ['testing', 'example'],
  py_modules=['bemesis'], # arbitrary keywords
  install_requires=install_requires,
  classifiers = [], )
