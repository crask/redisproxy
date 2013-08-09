memcache_client is a minimal, pure python client for memcached, kestrel, etc.
It is an alternative to the de-facto pure python standard client
'python-memcached', but is not intended to be a drop-in replacement.
The main features are better timeout handling and enforcement of bytestring keys and values.

 * Minimal API
   * Only supports set, get, delete, and stats
   * Client connects to single host -- no sharding over slabs
   * TCP sockets only -- no UDP or UNIX sockets
   * bytestring keys/values only -- no pickling/casting
 * Additional functionality
   * Timeouts via socket.settimeout() for fail-fast behavior
   * Transparent retry for a few common cases
 * Pure python
   * Eventlet friendly

The API looks very similar to the other memcache clients:

    import memcache
    mc = memcache.Client("127.0.0.1", 11211, timeout=1, connect_timeout=5)
    mc.set("some_key", "Some value")
    value = mc.get("some_key")
    mc.delete("another_key")
