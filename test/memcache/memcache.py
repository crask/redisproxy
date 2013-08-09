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

'''
a minimal, pure python client for memcached, kestrel, etc.

Usage example::

    import memcache
    mc = memcache.Client("127.0.0.1", 11211, timeout=1, connect_timeout=5)
    mc.set("some_key", "Some value")
    value = mc.get("some_key")
    mc.delete("another_key")
'''

import errno
import re
import socket

class ClientException(Exception):
    '''
    Raised when the server does something we don't expect

    | This does not include `socket errors <http://docs.python.org/library/socket.html#socket.error>`_
    | Note that ``ValidationException`` subclasses this so, technically, this is raised on any error
    '''

    def __init__(self, msg, item=None):
        if item is not None:
            msg = '%s: %r' % (msg, item) # use repr() to better see special chars
        super(ClientException, self).__init__(msg)

class ValidationException(ClientException):
    '''
    Raised when an invalid parameter is passed to a ``Client`` function
    '''

    def __init__(self, msg, item):
        super(ValidationException, self).__init__(msg, item)

class Client(object):

    def __init__(self, host, port, timeout=None, connect_timeout=None):
        '''
        If ``connect_timeout`` is None, ``timeout`` will be used instead
        (for connect and everything else)
        '''
        self._addr = (host, port)
        self._timeout = timeout
        self._connect_timeout = connect_timeout
        self._socket = None

    def __del__(self):
        self.close()

    def _get_addr(self):
        return self._addr

    address = property(_get_addr)
    ''' A read-only (str, int) tuple representing the host operations are performed on '''

    def _get_timeout(self):
        return self._timeout

    def _set_timeout(self, timeout):
        # presumably this should fail rarely
        # set locally before on socket
        # b/c if socket fails, it will probably be closed/reopened
        # and will want to use last intended value
        self._timeout = timeout
        if self._socket:
            self._socket.settimeout(timeout)

    timeout = property(_get_timeout, _set_timeout)
    '''
    A float representing the timeout in seconds for reads and sends on the underlying socket
    (``connect_timeout`` cannot be changed once init)

    Setting a timeout can raise a ``TypeError`` (non-float)  or a ``ValueError`` (negative)
    '''

    def _connect(self):
        # buffer needed since we always ask for 4096 bytes at a time
        # thus, might read more than the current expected response
        # cleared on every reconnect since old bytes are part of old session and can't be reused
        self._buffer = ''

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        connect_timeout = self._connect_timeout if self._connect_timeout is not None else self._timeout
        self._socket.settimeout(connect_timeout) # passing None means blocking
        try:
            self._socket.connect(self._addr)
            self._socket.settimeout(self._timeout)
        except (socket.error, socket.timeout):
            self._socket = None # don't want to hang on to bad socket
            raise

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
                try:
                    tmp = self._socket.recv(4096)
                except (socket.error, socket.timeout) as e:
                    self.close()
                    raise e

                if not tmp:
                    # we handle common close/retry cases in _send_command
                    # however, this can happen if server suddenly goes away
                    # (e.g. restarting memcache under sufficient load)
                    raise socket.error, 'unexpected socket close on recv'
                else:
                    self._buffer += tmp
        return result

    def _send_command(self, command):
        '''
        Send command to server and return initial response line
        Will reopen socket if it got closed (either locally or by server)
        '''
        if self._socket: # try to find out if the socket is still open
            try:
                self._socket.settimeout(0)
                self._socket.recv(0)
                # if recv didn't raise, then the socket was closed or there is junk
                # in the read buffer, either way, close
                self.close()
            except socket.error as e:
                if e.errno == errno.EAGAIN: # this is expected if the socket is still open
                    self._socket.settimeout(self._timeout)
                else:
                    self.close()

        if not self._socket:
            self._connect()

        self._socket.sendall(command)
        return self._read()

    # key supports ascii sans space and control chars
    # \x21 is !, right after space, and \x7e is -, right before DEL
    # also 1 <= len <= 250 as per the spec
    _valid_key_re = re.compile('^[\x21-\x7e]{1,250}$')

    def _validate_key(self, key):
        if not isinstance(key, str): # avoid bugs subtle and otherwise
            raise ValidationException('key must be str', key)
        m = self._valid_key_re.match(key)
        if m:
            # in python re, $ matches either end of line or right before
            # \n at end of line. We can't allow latter case, so
            # making sure length matches is simplest way to detect
            if len(m.group(0)) != len(key):
                raise ValidationException('trailing newline', key)
        else:
            raise ValidationException('invalid key', key)
        return key

    def close(self):
        '''
        Closes the socket if its open

        | Sockets are automatically closed when the ``Client`` object is garbage collected
        | Sockets are opened the first time a command is run (such as ``get`` or ``set``)
        | Raises socket errors
        '''
        if self._socket:
            self._socket.close()
            self._socket = None

    def delete(self, key):
        '''
        Deletes a key/value pair from the server

        Raises ``ClientException`` and socket errors
        '''
        # req  - delete <key> [noreply]\r\n
        # resp - DELETED\r\n
        #        or
        #        NOT_FOUND\r\n
        key = self._validate_key(key)

        command = 'delete %s\r\n' % key
        resp = self._send_command(command)
        if resp != 'DELETED\r\n' and resp != 'NOT_FOUND\r\n':
            raise ClientException('delete failed', resp)

    def get(self, key):
        '''
        Gets a single value from the server; returns None if there is no value

        Raises ``ValidationException``, ``ClientException``, and socket errors
        '''
        return self.multi_get([key])[0]

    def multi_get(self, keys):
        '''
        Takes a list of keys and returns a list of values

        Raises ``ValidationException``, ``ClientException``, and socket errors
        '''
        if len(keys) == 0:
            return []

        # req  - get <key> [<key> ...]\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        keys = [self._validate_key(key) for key in keys]
        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')
        command = 'get %s\r\n' % ' '.join(keys)
        received = {}
        resp = self._send_command(command)
        error = None

        while resp != 'END\r\n':
            terms = resp.split()
            if len(terms) == 4 and terms[0] == 'VALUE': # exists
                key = terms[1]
                flags = int(terms[2])
                length = int(terms[3])
                if flags != 0:
                    error = ClientException('received non zero flags')
                val = self._read(length+2)[:-2]
                if key in received:
                    error = ClientException('duplicate results from server')
                received[key] = val
            else:
                raise ClientException('get failed', resp)
            resp = self._read()

        if error is not None:
            # this can happen if a memcached instance contains items set by a previous client
            # leads to subtle bugs, so fail fast
            raise error

        if len(received) > len(keys):
            raise ClientException('received too many responses')
        # memcache client is used by other servers besides memcached.
        # In the case of kestrel, responses coming back to not necessarily
        # match the requests going out. Thus we just ignore the key name
        # if there is only one key and return what we received.
        if len(keys) == 1 and len(received) == 1:
            response = received.values()
        else:
            response = [received.get(key) for key in keys]
        return response

    def set(self, key, val, exptime=0):
        '''
        Sets a key to a value on the server with an optional exptime (0 means don't auto-expire)

        Raises ``ValidationException``, ``ClientException``, and socket errors
        '''
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)
        key = self._validate_key(key)

        # the problem with supporting types is it oftens leads to uneven and confused usage
        # some code sites use the type support, others do manual casting to/from str
        # worse yet, some sites don't even know what value they are putting in and mis-cast on get
        # by uniformly requiring str, the end-use code is much more uniform and legible
        if not isinstance(val, str):
            raise ValidationException('value must be str', val)

        # typically, if val is > 1024**2 bytes server returns:
        #   SERVER_ERROR object too large for cache\r\n
        # however custom-compiled memcached can have different limit
        # so, we'll let the server decide what's too much

        if not isinstance(exptime, int):
            raise ValidationException('exptime not int', exptime)
        elif exptime < 0:
            raise ValidationException('exptime negative', exptime)

        command = 'set %s 0 %d %d\r\n%s\r\n' % (key, exptime, len(val), val)
        resp = self._send_command(command)
        if resp != 'STORED\r\n':
            raise ClientException('set failed', resp)

    def stats(self, additional_args=None):
        '''
        Runs a stats command on the server.

        ``additional_args`` are passed verbatim to the server.
        See `the memcached wiki <http://code.google.com/p/memcached/wiki/NewCommands#Statistics>`_ for details
        or `the spec <https://github.com/memcached/memcached/blob/master/doc/protocol.txt>`_ for even more details

        Raises ``ClientException`` and socket errors
        '''
        # req  - stats [additional args]\r\n
        # resp - STAT <name> <value>\r\n (one per result)
        #        END\r\n
        if additional_args is not None:
            command = 'stats %s\r\n' % additional_args
        else:
            command = 'stats\r\n'

        resp = self._send_command(command)
        result = {}
        while resp != 'END\r\n':
            terms = resp.split()
            if len(terms) == 2 and terms[0] == 'STAT':
                result[terms[1]] = None
            elif len(terms) == 3 and terms[0] == 'STAT':
                result[terms[1]] = terms[2]
            else:
                raise ClientException('stats failed', resp)
            resp = self._read()
        return result
