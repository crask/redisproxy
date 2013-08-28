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


class TestDelete(unittest.TestCase):
    @classmethod
    def setUpClass(c):
        c.conf = load_conf("nutcracker.yml")

    def new_pool_client(self, name):
        port = int(parse_port(self.conf[name]['listen']))
        client = mc.Client('127.0.0.1', port)
        client._connect()
        return client

    @classmethod
    def tearDownClass(c):
        pass
    
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
#        self.beta_server = self.new_server_client('beta_cluster')
        self.beta_gutter = self.new_pool_client('beta_gutter')
        
        self.gamma = self.new_pool_client('gamma')
        
        self.mcq = self.new_redis_pool_client('mcq')

    def tearDown(self):
        self.alpha.close()
        self.alpha_server.close()
        self.alpha_gutter.close()
        
        self.beta.close()
#        self.beta_server.close()
        self.beta_gutter.close()

        self.gamma.close()

    def test_delete(self):
        for i in range(1):
            key = id_generator()
            val = id_generator()

            self.alpha.set(key, val)
#            self.beta.set(key, val)

            self.alpha.delete(key)

            mcval = self.alpha.get(key)
            self.assertEqual(mcval, None)

            time.sleep(2)
#            mcval = self.beta.get(key)
#            self.assertEqual(mcval, None)
            

if __name__ == '__main__':
    suite = unittest.TestSuite([
        unittest.TestLoader().loadTestsFromTestCase(TestDelete)
    ])
    
    unittest.TextTestRunner(verbosity=2).run(suite)

