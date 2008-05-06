#!/usr/bin/env python

from distutils.core import setup

description_long = '''
A client library for beanstalkd. beanstalkd is a lightweight queuing daemon
based on libevent. It is meant to be used across multiple systems, and takes
inspiration from memcache. More details on beanstalk can be found at:
http://xph.us/software/beanstalkd/

This client library aims to be simple and extensible. It curently provides no
fancy wrappers, or e.g. thread-wrangling
'''

setup(name='pybeanstalk',
      version='0.11.1',
      description='A python client library for beanstalkd.',
      long_description = description_long,
      author='Erich Heine',
      author_email='sophacles@gmail.com',
      url='http://code.google.com/p/pybeanstalk',
      classifiers=[
        'Development Status :: 3 - Alpha',
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
      packages=['beanstalk']
)

