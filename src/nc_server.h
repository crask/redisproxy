/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_SERVER_H_
#define _NC_SERVER_H_

#include <nc_core.h>

/*
 * server_pool is a collection of servers and their continuum. Each
 * server_pool is the owner of a single proxy connection and one or
 * more client connections. server_pool itself is owned by the current
 * context.
 *
 * Each server is the owner of one or more server connections. server
 * itself is owned by the server_pool.
 *
 *  +-------------+
 *  |             |<---------------------+
 *  |             |<------------+        |
 *  |             |     +-------+--+-----+----+--------------+
 *  |   pool 0    |+--->|          |          |              |
 *  |             |     | server 0 | server 1 | ...     ...  |
 *  |             |     |          |          |              |--+
 *  |             |     +----------+----------+--------------+  |
 *  +-------------+                                             //
 *  |             |
 *  |             |
 *  |             |
 *  |   pool 1    |
 *  |             |
 *  |             |
 *  |             |
 *  +-------------+
 *  |             |
 *  |             |
 *  .             .
 *  .    ...      .
 *  .             .
 *  |             |
 *  |             |
 *  +-------------+
 *            |
 *            |
 *            //
 */

typedef uint32_t (*hash_t)(const char *, size_t);

struct continuum {
    uint32_t index;  /* server index */
    uint32_t value;  /* hash value */
};

#define NC_SERVER_READABLE    (0x01)
#define NC_SERVER_WRITABLE    (0x02)

struct server {
    uint32_t            idx;   /* server index */
    struct server_pool *owner; /* owner pool */

    struct string    pname;    /* name:port:weight (ref in conf_server) */
    struct string    name;     /* name (ref in conf_server) */
    uint16_t         port;     /* port */
    uint32_t         weight;   /* weight */
    uint32_t         flags;  /* weight */

    int              family;   /* socket family */
    socklen_t        addrlen;  /* socket length */
    struct sockaddr *addr;     /* socket address (ref in conf_server) */

    uint32_t        ns_conn_q; /* # server connection */
    struct conn_tqh s_conn_q;  /* server connection q */

    int64_t  next_retry;       /* next retry time in usec */
    uint32_t failure_count;    /* # consecutive failures */
    int64_t last_failure;      /* last failure time in usec */

    int range_start;           /* range start */
    int range_end;             /* range end */

    int64_t next_probe;        /* next probe time in usec */
    
    void *stats;               /* stats data */
};

struct downstream_pool {
    struct string    ns;       /* namespace */
    struct string    name;     /* pool name */                               
};

struct server_pool {
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */
    
    struct conn        *p_conn;              /* proxy connection (listener) */
    uint32_t           nc_conn_q;            /* # client connection */
    struct conn_tqh    c_conn_q;             /* client connection q */

    struct array       server;               /* server[] */
    uint32_t           ncontinuum;           /* # continuum points */
    uint32_t           nserver_continuum;    /* # servers - live and dead on continuum (const) */
    struct continuum   *continuum;           /* continuum */
    uint32_t           nlive_server;         /* # live server */
    int64_t            next_rebuild;         /* next distribution rebuild time in usec */

    struct array       partition;            /* continuum[][] */
    uint32_t           npartition_continuum;
    struct array       *partition_continuum;
    struct array       r_partition_continuum;/* readable partition continuum */
    struct array       w_partition_continuum;/* writable partition continuum */

    struct string      name;                 /* pool name (ref in conf_pool) */
    struct string      addrstr;              /* pool address (ref in conf_pool) */
    uint16_t           port;                 /* port */
    int                family;               /* socket family */
    socklen_t          addrlen;              /* socket length */
    struct sockaddr    *addr;                /* socket address (ref in conf_pool) */
    int                dist_type;            /* distribution type (dist_type_t) */
    int                key_hash_type;        /* key hash type (hash_type_t) */
    hash_t             key_hash;             /* key hasher */
    struct string      hash_tag;             /* key hash tag (ref in conf_pool) */
    int                timeout;              /* timeout in msec */
    int                backlog;              /* listen backlog */
    uint32_t           client_connections;   /* maximum # client connection */
    uint32_t           server_connections;   /* maximum # server connection */
    int64_t            server_retry_timeout; /* server retry timeout in usec */
    uint32_t           server_failure_limit; /* server failure limit */
    int64_t            server_failure_interval; /* server failure interval */
    unsigned           auto_eject_hosts:1;   /* auto_eject_hosts? */
    unsigned           preconnect:1;         /* preconnect? */
    unsigned           redis:1;              /* redis? */

    unsigned           auto_probe_hosts:1;   /* auto_probe_hosts? */

    unsigned           auto_warmup:1;        /* auto_warmup? */

    struct string      gutter_name;          /* gutter pool name */
    struct server_pool *gutter;              /* gutter pool */

    struct string      peer_name;            /* peer pool name */
    struct server_pool *peer;                /* peer pool */

    struct array       downstreams;          /* downstreams*/
    struct hash_table  *downstream_table;    /* downstream table */

    unsigned           virtual:1;            /* virtual server */        
    struct string      namespace;            /* namespace */

    float              rate;                 /* # of request per second*/
    float              burst;                /* max bursts of requests */
    float              count;                /* # of request in the bucket */

    struct string      message_queue_name;   /* name of message queue */
    struct server_pool *message_queue;       /* message queue */
};

void server_ref(struct conn *conn, void *owner);
void server_unref(struct conn *conn);
int server_timeout(struct conn *conn);
bool server_active(struct conn *conn);
rstatus_t server_init(struct array *server, struct array *conf_server, struct server_pool *sp);
void server_deinit(struct array *server);
struct conn *server_conn(struct server *server);
rstatus_t server_connect(struct context *ctx, struct server *server, struct conn *conn);
void server_close(struct context *ctx, struct conn *conn);
void server_connected(struct context *ctx, struct conn *conn);
void server_ok(struct context *ctx, struct conn *conn);

struct conn *server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key, uint32_t keylen);
rstatus_t server_pool_run(struct server_pool *pool);
rstatus_t server_pool_preconnect(struct context *ctx);
void server_pool_disconnect(struct context *ctx);
rstatus_t server_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
void server_pool_deinit(struct array *server_pool);

void server_pool_probe(struct context *ctx);
void server_pool_update_quota(struct context *ctx);
bool server_pool_ratelimit(struct server_pool *pool);

static inline
void use_writable_pool(struct server_pool *pool) {
    pool->partition_continuum = &pool->w_partition_continuum;
}

static inline
void use_readable_pool(struct server_pool *pool) {
    pool->partition_continuum = &pool->r_partition_continuum;
}

#endif
