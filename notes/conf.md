# 配置说明

* listen: 监听地址和端口，格式为name:port或ip:port
* hash: 所使用的hash方法
  * one\_at\_a\_time
  * md5
  * crc16
  * crc32
  * crc32a
  * fnv1\_64
  * fnv1a\_64
  * fnv1\_32
  * fnv1a\_32
  * hsieh
  * murmur
  * murmur\_64 (与redisproxy所使用的hash方法兼容)
  * jenkins
  
* hash\_tag: 格式为由两个字符所组成的字符串，例如"{}"或"$$"，用于指定
  将key的某一部分作为hash函数的输入
* distribution: key分布算法，可能值为：
  * ketama 一致性哈希
  * modula 取模
  * random 随机
  * range 按区间分布
* timeout: 连接超时及读超市，单位ms。默认为无限长时间。
* backlog: TCP backlog参数，默认512。
* preconnect: true或false，表示nutcracker启动的时候是否预先与后端所有
  server建立连接。默认false。
* redis: true或false，表示使用redis或者memcached协议，默认为false。
* server\_connections: 与每个后端server所能建立的最大连接数，默认为1.
* auto\_eject\_hosts: true或false，表示在server失败时，是否暂时性的屏
  蔽该server，默认为false
* server\_retry\_timeout: 在auto\_eject\_hosts为true的情况下，server被
  封禁后，等待多长之后进行重试，默认为30000ms
* servers: 后端server列表，格式为name:port:weight或ip:port:weight，以
  及与具体ditribution方法相关的若干可选参数

# Redis示例配置

```
alpha:
  listen: 0.0.0.0:22121
  hash: murmur_64
  distribution: range
  auto_eject_hosts: true
  redis: true
  server_retry_timeout: 2000
  server_failure_limit: 1
  servers:
   - 127.0.0.1:6379:1 server1 0-32768     # 分片1
   - 127.0.0.1:6380:1 server2 0-32768     # 分片1
   - 127.0.0.1:6381:1 server3 32768-65536 # 分片2
   - 127.0.0.1:6382:1 server4 32768-65536 # 分片2

```

`distribution: range`模式下，需要为server配置
[0, 65536)中的某个区间。区间配置相同的server将被划分为同一分片，分片内部的一主多从将以random的方式进行负载均衡。

