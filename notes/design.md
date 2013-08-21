# 简要介绍

twemproxy的主要特点为：

1. 高性能
2. 轻量级
3. 复用后端连接，能够有效的降低后端服务的连接数
4. 支持memcache和redis协议，并且可以进一步扩展
5. 支持请求、响应的pipeline
6. 支持数据切分，为memcached和redis提供集群功能

能够很好的满足我们对proxy的基本需求，除此之外我们还需要以下功能：

1. Range Sharding，按照范围的方式进行数据拆分
2. Server Probe，对后端状态进行探测
3. Failover，自动故障处理
4. Virtual Server Pool，虚拟集群
5. Warmup，冷实例启动时热cache
6. 延迟删除

# 设计原则

1. 尽量符合twemproxy现有架构，便于之后与其主干版本merge，持续更新
2. 尽量通用化，便于实现redis或其他协议类似需求

# 概要设计

## Range Sharding

### 基本逻辑

由于当在集群中的某个节点失效时，consistant hashing的rehash机制会带来很
大的脏数据风险，所以我们一般不使用rehash机制，即consistant hashing基本
等价于hash+range；并且，我们希望能够通过手动指定分区的方式来控制每个节
点的负载，综上mcproxy需要提供hash+ranging的方式，来对后端进行sharding。

目前twemproxy中提供了ketama、modula和random三种方式，我们需要在此基础
上提供range的方式。

### 具体实现

首先，我们要说明一下twemproxy现有的后端健康检查机制。twemproxy在每次请
求失败后，都会调用`server_failure()`对该server的健康状态进行更新，注意，
这里必须开启`auto_eject_hosts`，否则这里的健康检查机制都不会生效。每次
对`server_failure()`的调用都会增加`failure_count`，当该数值超过配置的
`server_failure_limit`时，就会调用`server_pool_run()`对该server进行封
禁。twemproxy中所有的distribution都会维护一个`continuum`结构，其中包含
了当前存活的server节点。当需要对一个server进行封禁时，只需要将其从
`continuum`中踢出即可。

但是在我们的range distribution中，我们不希望由于对server的封禁操作影响
到数据的分布，所以在对server进行封禁时，并不会将server剔除，而是在
`server_conn()`中对`next_retry > 0`进行判断，来决定该server是否处于封
禁状态。


### 配置变更

`server_pool`的`distribution`属性增加`range`的方式，`servers`具体配置
增加范围配置

```
backup:
  listen: 127.0.0.1:22121
  hash: murmur
  servers:
    - 127.0.0.1:22122:1 server1^0
    - 127.0.0.1:22123:1 server2^32768
  distribution: range
  broadcast_domain: some_pid
```

## Server Probe

### 基本逻辑

为了实现冷集群warmup等功能，我们需要能够间隔性的通过具体协议相关的
`stats`或`info`命令来获取后端更为丰富的状态信息，如该后端是否处于cold状
态，即：

* 通过定时任务定时向后端发送`stats`或`info`命令
* 根据响应更新后端状态信息

### 具体实现

#### 发送探测请求

twemproxy中的主事件循环为`core_loop()`，其中会周期性的对
`core_timeout()`进行调用，处理超时的请求。为了不与原有逻辑混杂，我们增
加了`core_tick()`函数，将所有新添加的周期性任务及`core_timeout()`全部
放到该函数中执行。

`core_tick()`中与Server Probe功能相关的函数为`server_pool_probe()`，其
中会向每个server发送状态探测请求，主要流程如下：

1. `server_pool_update()`，对后端的健康状态进行更新，主要是将渡过封禁
   期的server解禁
2. `server_conn()`，获取对应后端的连接。原本这个函数只是负责从连接池中
   获取一个`struct conn`，只有在内存分配失败的情况下才会返回`NULL`。我
   们在此基础上增加了对该server是否处于封禁状态的判断，如果是，则直接
   返回`NULL`并且将`errno`置为`ECONNREFUSED`（详细原因请参见Range
   Distribution设计）
3. `server_connect()`，与后端建立连接
4. `msg_build_probe()`，创建探测请求
5. `req_enqueue()`，发送请求

`msg_build_probe()`的主要流程如下：

1. `msg_get()`获取`struct msg`，这里要注意的是，我们对`msg_get(struct
   conn *conn, bool request, bool redis)`进行了修改，原本`conn`参数一
   定不能为`NULL`，即一个消息要么属于server conn，要么属于client conn，
   但我们的探测消息显然不属于这两类，即`msg->owner == NULL`用于区分一
   个消息是不是twemcache自己生成的特殊消息
2. `msg->build_probe()`，调用具体的redis或者memcache的处理函数，

### 配置变更

增加`auto_probe_hosts`配置项，用于决定是否开启server主动探测
```
root:
  servers:
   - 127.0.0.1:22121:1
  auto_probe_hosts: true
```

#### 处理探测响应

1. 为了不影响twemproxy的主要处理逻辑，我们在`rsp_forward()`中增加了对
`msg->pre_rsp_forward()`的调用，其中包含了与具体的应用协议相关的处理逻
辑等
2. 对`memcache_parse_rsp()`中的状态机进行修改，对`stats`的响应进行解析


## Gutter

### 基本逻辑

为了在后端出现问题的时候保证服务质量，我们需要能够在主机群连接失败时，
使用后备集群提供服务，如果获取后端连接失败，则尝试从后备集群中获取连接，
并按照正常流程继续处理

### 具体实现

1. 为`server_pool`添加`struct server_pool *backup`属性，指向配置中指定
的后备集群
2. 更改请求转发流程，当从主机群中获取连接失败时，从后备集群获取连接，
继续提供服务

基本逻辑非常简单，需要对`req_forward()`的流程进行更改，使其能够在无法
获取连接的情况下能够从gutter pool中获取连接进行处理。主要问题在于如何
对这部分逻辑进行抽象。

我们的做法是，将这部分逻辑抽象为`msg->routing()`，由具体的协议来负责实
现消息的路由逻辑。这样做就不会让memcache相关的逻辑将redis请求转发逻辑
搞乱。

### 配置变更

为每个`server_pool`增加`gutter`属性，表明后备集群的名称，注意，如果要
是用gutter，则必须同时开启`auto_eject_hosts`

```
root:
  servers:
   - 127.0.0.1:22121:1
  gutter: backup

backup:
  servers:
    - 127.0.0.1:22122:1
```

## Virtual Server Pool

### 基本逻辑

由于在twemproxy公共集群中，需要配置大量的端口，这会对运维造成很大的困
扰，所以我们希望通过virtual server pool来是用少数几个公共端口，根据请
求key中携带的信息，将其转发到不同的后端集群进行处理，以达到简化配置的
目的。


### 具体实现

首先，我们为`struct server_pool`增加`virtual`属性，用于表明他是不是一
个virtual server pool，在请求接收完成后，即在`req_recv_done()`中，通过
`pool->virtual`判断当前是否为virtual server pool，如果是的话，则调用
`req_virtual_forward()`进行处理。

在virtual server pool中会指定一系列的下游集群(downstream)及对应的
namespace，请求中通过hashtag来指定对应的namespace。

`req_virtual_forward()`会通过key中携带的namespace在
`pool->downstream_table`中对下游server pool进行查找，如果查找成功，则
该连接的所有权：

```
c_conn->unref(c_conn);
c_conn->ref(c_conn, downstream);
```

并进一步调用`req_forward()`进行具体的请求转发处理。


### 配置变更

在每一个想要启用该功能的server pool中需要指定对应的namespace

```
test.poolname:
    namespace: test.namespace
```

在virtual server pool中增加`virtual`及`downstreams`的配置，其中
`downstreams`中每一行的格式为`namespace poolname`。

virtual server pool必须配置`hash_tag`属性，用于在请求key中切分
namespace

```
virtual_pool:
  listen: 127.0.0.1:22125
  virtual: true
  hash_tag: "{}"
  downstreams:
    - test.namespace test.poolname
```


## Warmup

### 基本逻辑

由于memcached实例在启动后，在很长一段时间后才能正常提供服务，我们需要
mcproxy能够提供冷集群的warmup功能

1. 如果从主集群中获取的连接属性为cold，则尝试从后备集群中获取连接
2. 同时向主集群和后备集群发送请求，并将主机群的响应丢弃，该请求仅用于
   测试后端命中率
2. 收到后备集群的响应后，向客户端返回结果，并根据请求和响应组装warmup
请求，发送至主集群，不对响应进行处理

### 具体实现

首先，我们已经将请求路由的逻辑抽象为`msg->routing()`，所以选择连接的逻
辑自然要在这个函数中进行实现。这部分逻辑与failover部分的逻辑类似，都是
先尝试从住集群中获取连接，如果获取失败（这里是`memcache_cold(conn)`），
则从后备集群中尝试获取连接。唯一的不同之处在于，在warmup的场景下，选择
完后备集群的连接后，需要记录这条消息对应的主集群连接，以便于往主集群发
送测试请求，并且在从后备集群收到响应后向主集群发送warmup请求。这里的标
记是通过`msg->origin = s_conn`来完成的。


## 延迟删除

### 基本逻辑

由于在某些请求失败后，需要添加延迟删除任务，所以mcproxy需要能够与延迟
删除队列相连接，由于我们的延迟删除队列使用redis进行实现，所以我们可以
直接复用已有的与redis交互部分：

```
job_queue:
  listen: 127.0.0.1:22120
  redis: true
  distribution: ketama
  preconnect: true
  auto_eject_hosts: false
  servers:
   - 127.0.0.1:6379:1
   - 127.0.0.1:6370:1
```

即延迟删除队列对外完全就是一个`redis`的集群，为了降低单个延迟删除队列
的负载，mcproxy可以根据指定的分布方式将请求分配在各个后端实例上。

### 配置项变更

在配置项中，我们直接使用`server_pool`的名字来识别延迟删除队列，不需要
增加任何额外的配置项。

# 其他

## `struct msg->owner`

twemproxy中`msg->owner`只有两种可能：`client conn`或者`server conn`，
分别对应请求和响应消息。为了实现warmup和request broadcast的功能，我们
会有一些请求不属于任何的`client conn`，即`msg->owner = NULL`的情况。但
是对于响应消息`ASSERT(msg->owner != NULL)`成立。

## 主要处理函数

`conn->recv`和`conn->send`分别该连接读写事件对应的处理函数，会被
`core_recv()`和`core_send()`调用。

```
    conn->recv = msg_recv;
    conn->recv_next = req_recv_next;
    conn->recv_done = req_recv_done;
    
    conn->send = msg_send;
    conn->send_next = rsp_send_next;
    conn->send_done = rsp_send_done;
    
    conn->close = client_close;
    conn->active = client_active;
    
    conn->ref = client_ref;
    conn->unref = client_unref;
    
    conn->enqueue_inq = NULL;
    conn->dequeue_inq = NULL;
    conn->enqueue_outq = req_client_enqueue_omsgq;
    conn->dequeue_outq = req_client_dequeue_omsgq;
```

由于nc底层异步IO处理使用的是ET模型，所以必须在每次读写事件触发时，将全
部数据收、发完。

## 消息处理主循环

`msg_recv()`为消息处理主循环。

```
    conn->recv_ready = 1;
    do {
        msg = conn->recv_next(ctx, conn, true);
        if (msg == NULL) {
            return NC_OK;
        }

        status = msg_recv_chain(ctx, conn, msg);
        if (status != NC_OK) {
            return status;
        }
    } while (conn->recv_ready);
```

在每一次的迭代中，首先通过`conn->recv_next()`获取下一个待处理的`msg`，
然后调用`msg_recv_chain()`来进行数据收取、解析等操作。

`msg->mhdr`指向一串的`mbuf`，其中每个`mbuf`都是一个固定大小的内存块。

## 消息处理

`recv_msg_chain()`的流程如下：

1. 从`msg->mhdr`获取最后一个`mbuf`，如果该`mbuf`不存在或者已写满，则分
配一个新的`mbuf`并插入到`msg->mhdr`尾部
2. 调用`conn_recv()`从`conn`中读取数据，填满该`mbuf`
3. 调用`msg_parse()`对该`msg`目前为止收到的数据进行解析
4. 调用`conn->recv_next()`获取下一个待处理的`msg`，并跳转到步骤3

由于在步骤2中所读取的数据可能包含一部分下一个请求的数据，所以
`msg_parse()`在解析完毕当前请求后，会将之后的数据转移到一个新的`msg`中，
并将该`msg`设置为该`conn`的当前待处理消息。然后在步骤4中，
`conn->recv_next()`就会获取到这个新的`msg`，并跳转到步骤3进行处理。

即`msg_recv_chain()`会进行一次读IO操作，并对所有读到的消息依次进行处理。

## 消息解析

`msg_parse()`调用具体协议相关的`msg->parser`对消息进行处理，并根据其返回值进
行对应的处理：

* `MSG_PARSE_OK`，当前消息解析成功，调用`msg_parsed()`，将当前`mbuf`中
  剩余未解析数据转移到新的`mbuf`中，并创建新的`msg`，将两者相关联，最
  后调用`conn->recv_done`将当前`msg`转入后续处理流程，并将新创建的
  `msg`设置为`conn`当前待处理的消息
* `MSG_PARSE_FRAGMENT`， 表示当前消息为批量消息，即包含多个key，需要调
  用`msg_fragment()`将该消息进行拆分，将除第一个子请求外的其他数据转移
  至新的`mbuf`在后续进行处理
* `MSG_PARSE_REPAIR`， 表示需要在读取更多数据后，再次进行解析，并且请
  求当前正在被处理的`token`不完整，需要将该`token`已有部分数据转移到新
  的`mbuf`中，并在下次之后补充更多数据后再次进行处理。转移`token`的逻
  辑由`msg_repair()`完成
* `MSG_PARSE_AGAIN`，表示需要在读取更多数据后，再次进行解析，用于处理
  请求超过当前`mbuf`的可用空间。


## 消息转发

调用`conn->recv_done`，即`req_recv_done()`的时候，会对完成解析的消息进
行后续处理：

* 首先调用`req_filter()`对消息进行过滤
* 其次调用`req_forward()`对消息进行转发

`req_forward()`的具体逻辑为：

1. 如果该消息需要响应，则将其加入`c_conn->omsg_q`中，即发送待响应队列
2. 从连接池中获取对应的后端连接`s_conn`
3. 将该消息加入`s_conn->imsg_q`中，并注册写事件

## 向后端发送

当`conn`可写时，会触发`msg_send()`进行处理。该函数与`msg_recv()`类似，
通过不断循环调用`conn->send_next()`和`msg_send_chain()`对消息进行发送。

即当`s_conn`可写时，会触发`req_send_next()`，以获取下一个待发送的`msg`，
并调用`msg_send_chain()`进行写入。

`req_send_next()`的逻辑为，如果`conn->smsg`存在，则返回其下一个`msg`，
并将其设置为当前待发送的消息`conn->smsg`。

`msg_send_chain()`主要分为三个阶段：

1. 循环调用`conn->send_next()`获取下一个待发送消息，将其加入待发送队列
`send_msgq`中，并构建对应的`iovec`，直至将当前`conn`所有待发送消息都加
入到`send_msgq`
2. 调用`conn_sendv()`发送数据
3. 从头至尾遍历`send_msgq`，将其中完成发送的消息调用
`conn->send_done()`进行处理，而发送了部分数据的消息则需要对其
`mbuf->pos`进行对应的修改，使下次能够继续发送

其中`conn->send_done()`，即`req_send_done()`的主要逻辑为：

1. 将该消息从`conn->imsgq`中剔除
2. 如果该请求需要回复，则将该请求加入到`conn->omsgq`中，否则直接将其释
放

## 接收后端响应

当`s_conn`可读时，触发`msg_recv()`进行消息接受，逻辑大致同上面接收请求
消息的部分，除了获取下一个待处理消息变为`rsp_recv_next()`，且消息解析
部分变为使用的`msg->parser`变为对应协议的响应解析函数，如
`memcache_parse_rsp()`。

## 转发后端响应

解析完毕的消息会调用`rsp_forward()`进行转发处理。由于同一个`conn`中的
消息处理是一个FIFO的过程，所以当前的响应消息所对应的请求消息一定为
`TAILQ_FIRST(&s_conn->omsg_q)`，所以将该请求消息弹出，并且标记为完成
`pmsg->done=1`。这里需要注意的是，该请求消息还存在于所属客户端连接的待
响应队列`c_conn->omsg_q`中，所以需要检查该队列头部的请求是否完成，如果
完成的话，则注册写事件，发送响应。

```
// 将应答与对应请求相关联
pmsg->peer = msg;
msg->peer = pmsg;
```

由于这里会将请求及其对应的响应消息进行关联，所以在后续调用
`msg_send()`发送响应的时候，就可以通过`msg->peer`获取对应的响应消息。

## 向客户端发送响应

这部分不做赘述，与之前向后端发送请求的逻辑基本一致。

## 健康检查

发生读写错误的时候，最终会调用`core_close(ctx, conn)`来关闭对应的连接，该函数会进一步调用`conn->close(ctx, conn)`进行处理。

在`conn_get()`中，`conn->close`会被初始化为`client_close`或者`server_close`。

在`server_close(ctx, conn)`中，会调用`server_failure(ctx, conn)`中，来进行server健康状态的更新，该函数主要逻辑为：

1. `if (!pool->auto_eject_hosts) return;`
2. `server->failure_count++;`
3. `if (server->failure_count < pool->server_failure_limit) return;`
4. `server->failure_count = 0; server->next_retry = now + pool->server_retry_timeout;`
5. `server_pool_run(pool)`

其中`server_pool_run()`会调用具体的distribution对应的`xxx_update(pool)`对故障server进行封禁操作。

以`random_update(pool)`为例，具体过程为：

1. 遍历`server`列表，如果该`server`已经度过封禁时间
(`server->next_retry <= now`)，则可以将其解封(`server->next_retry =0`)，
否则则利用`server`的解禁时间(`next_retry`)来计算下一次`pool`更新的时间
(`next_rebuild`)
2. 经过步骤1，可以得到当前存活的`server`数量(`nlive_server`)，结合每个
`server`对应的虚节点数量，可以得出整个hash域的分布，这一数据会被对应的
`xxx_dispatch()`用于对每一个请求进行分发

在处理请求的过程中，获取对应`server`的连接是通过`server_pool_conn(ctx,
pool, key, keylen)`来完成的，具体过程为：

1. `server_pool_update(pool)`，间接调用`xxx_update(pool)`,对当前的
`server_pool`健康状况进行更新
2. `server_pool_server(pool, key, keylen)`，间接调用`xxx_dispath(...,
hash)`，来获得该请求对应的后端`server`
3. `server_conn(server)`，获取后端`server`对应的`struct conn`，这里只
会在内存分配失败的情况下才会返回`NULL`(我为了实现`range`算法中的均衡策
略，会在该函数中对`server`的封禁时间进行判断，如果该`server`处于封禁状
态，则直接返回`NULL`)
4. `server_connect(ctx, server, conn)`，如果上一步获取的`conn`并没有建
立实际的`socket`，则在该步中发起连接。由于在`connect`的时候该
`socket`就已经处于`nonblocking`模式，所以在返回后并非直接可用，需要等
待`WRITABLE`事件

