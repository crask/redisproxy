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

# this is a simple mock memcached server
# it only accepts a single cxn then closes
# the point is that we can handcraft delays
# to verify timeouts work as expected

import errno
from optparse import OptionError, OptionParser
import socket
import time

class SocketClosedException(Exception):

    def __init__(self):
        super(SocketClosedException, self).__init__('socket closed unexpectedly')

class MockMemcached(object):
    def __init__(self, host, port, accept_connections, get_delay):
        self._addr = (host, port)
        self._accept_connections = accept_connections
        self._get_delay = get_delay
        self._dict = {} # stores the key-val pairs
        self._root_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._root_socket.bind(self._addr)

        # buffer needed since we always ask for 4096 bytes at a time
        # and thus might read more than the current expected response
        self._buffer = ''
        # self._socket set after accept

    def _read(self, length=None):
        '''
        Return the next length bytes from server
        Or, when length is None,
        Read a response delimited by \r\n and return it (including \r\n)
        (Use latter only when \r\n is unambiguous -- aka for control responses, not data)
        '''
        result = None
        while result is None:
            if length: # length = 0 is ambiguous, so don't use 
                if len(self._buffer) >= length:
                    result = self._buffer[:length]
                    self._buffer = self._buffer[length:]
            else:
                delim_index = self._buffer.find('\r\n')
                if delim_index != -1:
                    result = self._buffer[:delim_index+2]
                    self._buffer = self._buffer[delim_index+2:]

            if result is None:
                tmp = self._socket.recv(4096)
                if not tmp:
                    raise SocketClosedException
                else:
                    self._buffer += tmp
        return result

    def _handle_get(self, key):
        # req  - get <key>\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        END\r\n
        if self._get_delay > 0:
            time.sleep(self._get_delay)
        if key in self._dict:
            val = self._dict[key]
            command = 'VALUE %s 0 %d\r\n%s\r\n' % (key, len(val), val)
            self._socket.sendall(command)
        self._socket.sendall('END\r\n')

    def _handle_set(self, key, length):
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)
        val = self._read(length+2)[:-2] # read \r\n then chop it off
        self._dict[key] = val
        self._socket.sendall('STORED\r\n')

    def run(self):
        self._root_socket.listen(1)

        if self._accept_connections:
            self._socket, addr = self._root_socket.accept()
        else:
            while True: # spin until killed
                time.sleep(1)

        while True:
            try:
                request = self._read()
                terms = request.split()
                if len(terms) == 2 and terms[0] == 'get':
                    self._handle_get(terms[1])
                elif len(terms) == 5 and terms[0] == 'set':
                    self._handle_set(terms[1], int(terms[4]))
                else:
                    print 'unknown command', repr(request)
                    break
            except SocketClosedException:
                print 'socket closed', repr(request)
                break

        self._socket.close()
        self._root_socket.close()

        # spin until killed - this simplifies cleanup code for the unit tests
        while True:
            time.sleep(1)

if __name__ == '__main__':
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage=usage)
    # note - this option cannot be used to test connect timeout
    # accept receives already-connected cxns that are ready to go
    parser.add_option(
        '--dont-accept',
        default=True,
        dest='accept_connections',
        action='store_false',
        help="don't accept any incoming connection requests",
    )
    parser.add_option(
        '--get-delay',
        default=0,
        dest='get_delay',
        metavar='GET_DELAY',
        type='int',
        help='delay get command by GET_DELAY seconds',
    )
    parser.add_option(
        '-p', '--port',
        default=11212,
        dest='port',
        metavar='PORT',
        type='int',
        help='listen on PORT',
    )
    (options, args) = parser.parse_args()
    if len(args) > 0:
        raise OptionError('unrecognized arguments: %s' % ' '.join(args))

    server = MockMemcached('127.0.0.1',
                           options.port,
                           options.accept_connections,
                           options.get_delay)
    server.run()
