#!/usr/bin/env python
import yaml
import re
import subprocess
import sys

def load_conf(filename):
    return yaml.load(open(filename).read())

def start_mcd(port, additional_args=[]):
    command = [
        'twemcache',
        '-p',
        str(port),
        '-o',
        'log/mcd.%s.log' % port,
    ]
    command.extend(additional_args)
    print ' '.join(command)
    return subprocess.Popen(command)

def killall_mcd():
    command = [
        'killall',
        'twemcache'
    ]
    print ' '.join(command)
    subprocess.call(command, stderr=open('/dev/null', 'w'))

def start_redis(port, additional_args=[]):
    command = [
        'redis-server',
        '--port',
        str(port)
    ]
    print ' '.join(command)
    return subprocess.Popen(command, stdout=open('/dev/null', 'w'))

def killall_redis():
    command = [
        'killall',
        'redis-server'
    ]
    print ' '.join(command)
    return subprocess.call(command, stderr=open('/dev/null', 'w'))

def start_mock_mcd(port, additional_args=[]):
    command = [
        './mock_mcd.py',
        '-p',
        str(port)
    ]
    command.extend(additional_args)
    print ' '.join(command)
    return subprocess.Popen(command)

def killall_mock_mcd():
    p = subprocess.Popen(['pgrep', '-f', 'mock_mcd'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    pids = out.split()
    if len(pids) == 0:
        return
    command = ['kill']
    command.extend(out.split())
    print ' '.join(command)
    subprocess.call(command)
    
def start_proxy(filename, additional_args=[]):
    command = [
        'nutcracker',
        '-c',
        filename,
        '-o',
        'log/proxy.log',
    ]
    command.extend(additional_args)
    print ' '.join(command)
    return subprocess.Popen(command)
    
def killall_proxy():
    command = [
        'killall',
        'nutcracker'
    ]
    print ' '.join(command)
    subprocess.call(command, stderr=open('/dev/null', 'w'))

def start_cluster(filename):
    proxy = start_proxy(filename, ['-v', '8'])

    conf = load_conf(filename)
    for server_pool_name in conf:
        server_pool = conf[server_pool_name]
        if 'virtual' not in server_pool:
            servers = server_pool['servers']
            for server in servers:
                tokens = re.split('[ |:]', server)
                host, port = tokens[0], tokens[1]
                if 'redis' in server_pool and server_pool['redis']:
                    start_redis(port)
                else:
                    start_mcd(port)
    return conf    

def stop_cluster():
    killall_mcd()
    killall_mock_mcd()
    killall_redis()
    killall_proxy()


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if sys.argv[1] == 'restart':
            stop_cluster()
            start_cluster('nutcracker.yml')
        elif sys.argv[1] == 'stop':
            stop_cluster()
        elif sys.argv[1] == 'start':
            start_cluster('nutcracker.yml')

