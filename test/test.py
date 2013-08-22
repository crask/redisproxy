#!/usr/bin/env python

import memcache as mc
import redis
import time
import unittest2 as unittest
import yaml
import string
import random

def id_generator(size=8, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def load_conf(filename):
    return yaml.load(open(filename).read())

def parse_port(addr):
    return addr.split(':')[1]

class TestBasic(unittest.TestCase):
    @classmethod
    def setUpClass(c):
        c.conf = load_conf('nutcracker.yml')
        
    @classmethod
    def tearDownClass(c):
        pass
    
    def new_pool_client(self, name):
        port = int(parse_port(self.conf[name]['listen']))
        client = mc.Client('127.0.0.1', port)
        client._connect()
        return client

    
    def new_server_client(self, name):
        port = int(parse_port(self.conf[name]['servers'][0]))
        client = mc.Client('127.0.0.1', port)
        client._connect()
        client._socket.sendall('flush_all noreply\r\n')
        return client

    def new_redis_pool_client(self, name):
        port = int(parse_port(self.conf[name]['listen']))
        client = redis.StrictRedis(host='localhost', port=port, db=0)
        return client

    def setUp(self):        
        self.alpha = self.new_pool_client('alpha_cluster')
        self.alpha_server = self.new_server_client('alpha_cluster')
        self.alpha_gutter = self.new_pool_client('alpha_gutter')

        self.beta = self.new_pool_client('beta_cluster')
        self.beta_server = self.new_server_client('beta_cluster')
        self.beta_gutter = self.new_pool_client('beta_gutter')
        
        self.gamma = self.new_pool_client('gamma')
        
        self.mcq = self.new_redis_pool_client('mcq')

    def tearDown(self):
        self.alpha.close()
        self.alpha_server.close()
        self.alpha_gutter.close()
        
        self.beta.close()
        self.beta_server.close()
        self.beta_gutter.close()

        self.gamma.close()
        
    def test_delete(self):
        key = 'delete'
        val = 'YBgGHw'
        self.alpha.set(key, val)
        mcval = self.alpha.get(key)
        self.assertEqual(mcval, val)
        self.alpha.delete(key)
        mcval = self.alpha.get(key)
        self.assertEqual(mcval, None)

    def test_multi_get(self):
        items = {'blob':'steam', 'help':'tres', 'HEHE':'Pans'}
        for k, v in items.items():
            self.alpha.set(k, v)
        resp = self.alpha.multi_get(items.keys())
        for v, r in zip(items.values(), resp):
            self.assertTrue(v == r)

    def test_expire(self):
        key = 'expire'
        val = "uiuokJ"
        self.alpha.set(key, val, exptime=1)
        time.sleep(2)
        mcval = self.alpha.get(key)
        self.assertEqual(mcval, None)

    def test_get_bad(self):
        self.assertRaises(Exception, self.alpha.get, 'get_bad\x84')
        mcval = self.alpha.get('!' * 250)
        self.assertEqual(mcval, None)
        self.assertRaises(Exception, self.alpha.get, '!' * 251)
        self.assertRaises(Exception, self.alpha.get, '')

        # this tests regex edge case specific to the impl
        self.assertRaises(Exception, self.alpha.get, 'get_bad_trailing_newline\n')

    def test_get_unknown(self):
        mcval = self.alpha.get('get_unknown')
        self.assertEqual(mcval, None)

    def test_set_bad(self):
        key = 'set_bad'
        self.assertRaises(Exception, self.alpha.set, key, '!' * 1024**2)
        self.alpha.set(key, '!' * (1024**2 - 100)) # not sure why 1024**2 - 1 rejected
        self.assertRaises(Exception, self.alpha.set, '', 'empty key')

    def test_set_get(self):
        key = 'set_get'
        val = "eJsiIU"
        self.alpha.set(key, val)
        mcval = self.alpha.get(key)
        self.assertEqual(mcval, val)

    def test_bad_flags(self):
        self.alpha._connect()
        key = 'badflags'
        val = 'xcHJFd'
        command = 'set %s 1 0 %d\r\n%s\r\n' % (key, len(val), val)
        self.alpha._socket.sendall(command)
        rc = self.alpha._read()
        self.assertEqual(rc, 'STORED\r\n')
        self.assertRaises(Exception, self.alpha.get, key)

    def test_str_only(self):
        self.assertRaises(Exception, self.alpha.set, u'unicode_key', 'sfdhjk')
        self.assertRaises(Exception, self.alpha.set, 'str_key', u'DFHKfl')

    def test_warmup(self):
        self.alpha_server._socket.sendall('cold 1\r\n')
        rc = self.alpha_server._read()
        self.assertEqual(rc, 'OK\r\n')

        probe_interval = int(self.conf['alpha_cluster']['server_retry_timeout'])/1000
        time.sleep(probe_interval)

        stats = self.alpha_server.stats()
        self.assertEqual(stats['cold'], '1')
        
        key = id_generator()
        val = id_generator()

        expire_time = 5
        self.beta.set(key, val, expire_time)
        mcval = self.alpha.getex(key)
        self.assertEqual(mcval, val)
        mcval = self.alpha_server.getex(key)
        self.assertEqual(mcval, val)
        
        time.sleep(expire_time)
        self.assertEqual(self.alpha.getex(key), None)
        
        self.beta.delete(key)
            
        self.alpha_server._socket.sendall('cold 0\r\n')
        rc = self.alpha_server._read()
        self.assertEqual(rc, 'OK\r\n')

        time.sleep(probe_interval)
        
        mcval = self.alpha.getex(key)
        self.assertEqual(mcval, val)
    
    def test_failover(self):
        pass

    def test_virtual_pool(self):
        key = id_generator()
        val = id_generator()
    
        self.gamma.set('{alpha_namespace}' + key, val)
        self.gamma.set('{beta_namespace}' + key, val)

        mcval = self.alpha.get('{alpha_namespace}' + key)
        self.assertEqual(mcval, val)

        mcval = self.beta.get('{beta_namespace}' + key)
        self.assertEqual(mcval, val)
    
    # def test_notify(self):
    #     key = id_generator()
        
    #     self.mcq.delete("alpha")

    #     self.alpha.delete(key)
    #     rval = self.mcq.lpop("alpha")
    #     self.assertEqual(rval, "delete %s" % key)
        
        
if __name__ == '__main__':
    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestBasic)
    ])
    
    unittest.TextTestRunner(verbosity=2).run(suite)
