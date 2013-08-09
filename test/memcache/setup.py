#!/usr/bin/env python

# This file is provided as a convenience for
# systems administrators, it does *not* represent
# a release on pypi.

from distutils.core import setup

setup(name='memcache_client',
      version='rolling',
      description='A minimal pure Python client for Memcached, Kestrel, etc.',
      url='https://github.com/mixpanel/memcache_client',
      py_modules=['memcache', 'mock_memcached', 'test'],
     )
