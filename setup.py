#!/usr/bin/env python

from setuptools import setup, find_packages

description_long = '''
A pure-Python client library for beanstalkd. beanstalkd is a lightweight queuing daemon
based on libevent. It is meant to be used across multiple systems, and takes
inspiration from memcache. More details on beanstalk can be found at:
http://xph.us/software/beanstalkd/

This client library aims to be simple and extensible. It provides both a single thread,
single connection serialized (select-based) beanstalk connection with optional, simple
thread pool implementation, and a basic Twisted client. Both can be used directly, or
be used as basis for for more sophisticated client applications. Please see the examples
directory for usage examples.

To install, just run python setup.py install from this directory, or use the shortcuts
in the Makefile. For Twisted support, run install with the 'twisted' extra.

The package home is at https://github.com/beanstalkd/pybeanstalk, with an issue tracker
and a wiki. Bug reports and pull requests most welcome.

'''

setup(name='pybeanstalk',
      version='1.0rc1',
      description='A python client library for beanstalkd.',
      long_description = description_long,
      keywords='beanstalkd, twisted',
      author='Erich Heine',
      author_email='sophacles@gmail.com',
      url='https://github.com/beanstalkd/pybeanstalk',
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Communications',
        'Topic :: Internet',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Object Brokering',
        'Topic :: System'],
      packages=['beanstalk'],
      install_requires=["pyaml"],
      extras_require={'twisted': ["zope.interface", "Twisted>=15.2.1"]},
      include_package_data=True,
      zip_safe=False
)

