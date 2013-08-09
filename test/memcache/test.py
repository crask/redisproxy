#!/usr/bin/env python

# Copyright 2012 Mixpanel, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    from . import memcache
except ValueError:
    import memcache

# subprocess is not monkey-patched, hence the special import
import sys
if 'eventlet' in sys.modules:
    from eventlet.green import subprocess
else:
    import subprocess

import os
import os.path
import socket
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest

low_port = 11000
high_port = 11210
# spin up new memcached instance to test against
def _start_new_memcached_server(port=None, mock=False, additional_args=[]):
    if not port:
        global low_port
        ports = range(low_port, high_port + 1)
        low_port += 1
    else:
        ports = [port]

    # try multiple ports so that can cleanly run tests 
    # w/o having to wait for a particular port to free up
    for attempted_port in ports:
        try:
            if mock:
                command = [
                    'python',
                    os.path.join(os.path.dirname(__file__), 'mock_memcached.py'),
                    '-p',
                    str(attempted_port),
                ]
            else:
                command = [
                    '/usr/bin/memcached',
                    '-p',
                    str(attempted_port),
                    '-m',
                    '1', # 1MB
                    '-l',
                    '127.0.0.1',
                ]
            command.extend(additional_args)
            p = subprocess.Popen(command)
            time.sleep(2) # needed otherwise unittest races against startup
            return p, attempted_port
        except:
            pass # try again
    else:
        raise Exception('could not start memcached -- no available ports')

# test memcache client for basic functionality
class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(c):
        c.memcached, c.port = _start_new_memcached_server()

    @classmethod
    def tearDownClass(c):
        try:
            c.memcached.terminate()
        except:
            print 'for some reason memcached not running'
        c.memcached.wait()

    def setUp(self):
        self.client = memcache.Client('127.0.0.1', self.port)

    def tearDown(self):
        self.client.close()

    def test_delete(self):
        key = 'delete'
        val = 'YBgGHw'
        self.client.set(key, val)
        mcval = self.client.get(key)
        self.assertEqual(mcval, val)
        self.client.delete(key)
        mcval = self.client.get(key)
        self.assertEqual(mcval, None)

    def test_multi_get(self):
        items = {'blob':'steam', 'help':'tres', 'HEHE':'Pans'}
        for k, v in items.items():
            self.client.set(k, v)
        resp = self.client.multi_get(items.keys())
        for v, r in zip(items.values(), resp):
            self.assertTrue(v == r)

    def test_expire(self):
        key = 'expire'
        val = "uiuokJ"
        self.client.set(key, val, exptime=1)
        time.sleep(2)
        mcval = self.client.get(key)
        self.assertEqual(mcval, None)

    def test_get_bad(self):
        self.assertRaises(Exception, self.client.get, 'get_bad\x84')
        mcval = self.client.get('!' * 250)
        self.assertEqual(mcval, None)
        self.assertRaises(Exception, self.client.get, '!' * 251)
        self.assertRaises(Exception, self.client.get, '')

        # this tests regex edge case specific to the impl
        self.assertRaises(Exception, self.client.get, 'get_bad_trailing_newline\n')

    def test_get_unknown(self):
        mcval = self.client.get('get_unknown')
        self.assertEqual(mcval, None)

    def test_set_bad(self):
        key = 'set_bad'
        self.assertRaises(Exception, self.client.set, key, '!' * 1024**2)
        self.client.set(key, '!' * (1024**2 - 100)) # not sure why 1024**2 - 1 rejected
        self.assertRaises(Exception, self.client.set, '', 'empty key')

    def test_set_get(self):
        key = 'set_get'
        val = "eJsiIU"
        self.client.set(key, val)
        mcval = self.client.get(key)
        self.assertEqual(mcval, val)

    def test_stats(self):
        stats = self.client.stats()
        self.assertTrue('total_items' in stats)

    def test_bad_flags(self):
        self.client._connect()
        key = 'badflags'
        val = 'xcHJFd'
        command = 'set %s 1 0 %d\r\n%s\r\n' % (key, len(val), val)
        self.client._socket.sendall(command)
        rc = self.client._read()
        self.assertEqual(rc, 'STORED\r\n')
        self.assertRaises(Exception, self.client.get, key)

    def test_str_only(self):
        self.assertRaises(Exception, self.client.set, u'unicode_key', 'sfdhjk')
        self.assertRaises(Exception, self.client.set, 'str_key', u'DFHKfl')

# make sure timeout works by using mock server
# test memcached failing in a variety of ways, coming back vs. not, etc
class TestFailures(unittest.TestCase):

    def test_gone(self):
        mock_memcached, port = _start_new_memcached_server()
        try:
            client = memcache.Client('127.0.0.1', port)
            key = 'gone'
            val = 'QWMcxh'
            client.set(key, val)

            mock_memcached.terminate()
            mock_memcached.wait()
            mock_memcached = None

            self.assertRaises(Exception, client.get, key)
            client.close()
        finally:
            if mock_memcached:
                mock_memcached.terminate()
                mock_memcached.wait()

    def test_hardfail(self):
        mock_memcached, port = _start_new_memcached_server()
        try:
            client = memcache.Client('127.0.0.1', port)
            key = 'hardfail'
            val = 'FuOIdn'
            client.set(key, val)

            mock_memcached.kill() # sends SIGKILL
            mock_memcached.wait()
            mock_memcached, port = _start_new_memcached_server(port=port)

            mcval = client.get(key)
            self.assertEqual(mcval, None) # val lost when restarted
            client.close()
        finally:
            mock_memcached.terminate()
            mock_memcached.wait()

class TestTimeout(unittest.TestCase):

    # make sure mock server works
    def test_set_get(self):
        mock_memcached, port = _start_new_memcached_server(mock=True)
        try:
            client = memcache.Client('127.0.0.1', port)
            key = 'set_get'
            val = 'DhuWmC'
            client.set(key, val)
            mcval = client.get(key)
            self.assertEqual(val, mcval)
            client.close()
        finally:
            mock_memcached.terminate()
            mock_memcached.wait()

    def test_get_timeout(self):
        mock_memcached, port = _start_new_memcached_server(mock=True, additional_args=['--get-delay', '2'])
        try:
            client = memcache.Client('127.0.0.1', port, timeout=1)
            key = 'get_timeout'
            val = 'cMuBde'
            client.set(key, val)
            # when running unpatched eventlet,
            # the following will fail w/ socket.error, EAGAIN
            self.assertRaises(socket.timeout, client.get, key)
            client.close()
        finally:
            mock_memcached.terminate()
            mock_memcached.wait()

class TestConnectTimeout(unittest.TestCase):

    # to run these tests, you need specify an ip that will not allow tcp from your machine to 11211
    # this is easiest way to test connect timeout, since has to happen at kernel level (iptables etc)
    unavailable_ip = '173.193.164.107' # appstage01 (external ip is firewalled, internal is not)

    def test_connect_timeout(self):
        # using normal timeout

        # client usually does lazy connect, but we don't want to confuse connect and non-connect timeout
        # so connect manually
        client = memcache.Client(self.unavailable_ip, 11211, timeout=1)
        self.assertRaises(socket.timeout, client._connect)
        client.close()

    def test_connect_timeout2(self):
        # using connect timeout
        client = memcache.Client(self.unavailable_ip, 11211, connect_timeout=1)
        self.assertRaises(socket.timeout, client._connect)
        client.close()

if __name__ == '__main__':
    # uncomment to only run specific tests
    #
    # sadly, the particular map below is a bit cryptic
    # basically, constructing the test case class with a string containing a method name
    # creates an instance of the class that will only run that test
    # a list of these can be passed to the test suite constructor to make the suite
    # suite = unittest.TestSuite(map(TestConnectTimeout, ['test_connect_timeout', 'test_connect_timeout2']))

    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestClient),
        unittest.TestLoader().loadTestsFromTestCase(TestFailures),
        unittest.TestLoader().loadTestsFromTestCase(TestTimeout),
        # TestConnectTimeout not part of normal suite -- requires special config
    ])

    unittest.TextTestRunner(verbosity=2).run(suite)
