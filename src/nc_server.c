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

#include <stdlib.h>
#include <unistd.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_conf.h>
#include <nc_proto.h>

static rstatus_t
server_dump(void *elem, void *data)
{
    struct server *s = elem;
    
    log_debug(LOG_DEBUG, "%.*s range [%"PRIu32", %"PRIu32")",
              s->pname, s->range_start, s->range_end);

    return NC_OK;
}

void
server_ref(struct conn *conn, void *owner)
{
    struct server *server = owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner == NULL);

    conn->family = server->family;
    conn->addrlen = server->addrlen;
    conn->addr = server->addr;

    server->ns_conn_q++;
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, server,
              server->pname.len, server->pname.data);
}

void
server_unref(struct conn *conn)
{
    struct server *server;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    server = conn->owner;
    conn->owner = NULL;

    ASSERT(server->ns_conn_q != 0);
    server->ns_conn_q--;
    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
              server->pname.len, server->pname.data);
}

int
server_timeout(struct conn *conn)
{
    struct server *server;
    struct server_pool *pool;

    ASSERT(!conn->client && !conn->proxy);

    server = conn->owner;
    pool = server->owner;

    return pool->timeout;
}

bool
server_active(struct conn *conn)
{
    ASSERT(!conn->client && !conn->proxy);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

    return false;
}

bool
server_cold(struct conn *conn)
{
    struct server *server;
    struct server_pool *pool;
    
    ASSERT(!conn->client && !conn->proxy);
    
    server = conn->owner;
    pool = server->owner;

    if (!pool->redis && pool->auto_warmup) {
        return memcache_cold(server->stats);
    } else {
        return false;
    }
}

static rstatus_t
server_each_set_owner(void *elem, void *data)
{
    struct server *s = elem;
    struct server_pool *sp = data;

    s->owner = sp;

    return NC_OK;
}

static rstatus_t
server_each_set_stats(void *elem, void *data)
{
    struct server *s = elem;
    struct server_pool *sp = data;

    if (sp->redis) {
        s->stats = memcache_create_stats();
    } else {
        s->stats = redis_create_stats();
    }

    if (s->stats == NULL) {
        return NC_ENOMEM;
    }        
    
    return NC_OK;
}


static int
server_compare(const void *lhs, const void *rhs)
{
    struct server *ls = (struct server *)lhs, *rs = (struct server *)rhs;

    return (ls->range_start - rs->range_start);
}


rstatus_t
server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    rstatus_t status;
    uint32_t nserver, i;
    struct server *cur, *next;

    nserver = array_n(conf_server);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server, nserver, sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf server to server */
    status = array_each(conf_server, conf_server_each_transform, server);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    ASSERT(array_n(server) == nserver);

    if (sp->dist_type == DIST_RANGE) {
        /* sort the servers according to range_start */
        array_sort(server, server_compare);
        
        for (i = 0; i < nserver - 1; i++) {
            cur = array_get(server, i);
            next = array_get(server, i + 1);            
            if (cur->range_start >= next->range_start
                || cur->range_start >= DIST_RANGE_MAX) {
                log_error("invalid range start: %.*s^%"PRIu32, 
                          sp->name.len, sp->name.data, cur->range_start);
                return NC_ERROR;
            }
            /* range is [start, end) */
            cur->range_end = next->range_start;
        }
        
        cur = array_get(server, nserver - 1);
        if (cur->range_start > DIST_RANGE_MAX) {
            return NC_ERROR;
        }
        cur->range_end = DIST_RANGE_MAX;
    }

    array_each(server, server_dump, NULL);
    /* set server owner */
    status = array_each(server, server_each_set_owner, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    status = array_each(server, server_each_set_stats, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }
    
    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

void
server_deinit(struct array *server)
{
    uint32_t i, nserver;

    for (i = 0, nserver = array_n(server); i < nserver; i++) {
        struct server *s;
        struct server_pool *pool;

        s = array_pop(server);
        pool = s->owner;
        
        ASSERT(pool->virtual || s->stats != NULL);
        ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);

        if (pool->redis) {
            redis_destroy_stats(s->stats);
        } else {
            memcache_destroy_stats(s->stats);
        }
    }
    array_deinit(server);
}

struct conn *
server_conn(struct server *server)
{
    struct server_pool *pool;
    struct conn *conn;

    /*
     * If next_retry <= now, it must have been reset to 0 by server_pool_update
     */
    if (server->next_retry > 0) {
        errno = ECONNREFUSED;
        return NULL;
    }
    
    pool = server->owner;

    /*
     * FIXME: handle multiple server connections per server and do load
     * balancing on it. Support multiple algorithms for
     * 'server_connections:' > 0 key
     */

    if (server->ns_conn_q < pool->server_connections) {
        return conn_get(server, false, pool->redis);
    }
    ASSERT(server->ns_conn_q == pool->server_connections);

    /*
     * Pick a server connection from the head of the queue and insert
     * it back into the tail of queue to maintain the lru order
     */
    conn = TAILQ_FIRST(&server->s_conn_q);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    return conn;
}

static rstatus_t
server_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server *server;
    struct server_pool *pool;
    struct conn *conn;

    server = elem;
    pool = server->owner;

    conn = server_conn(server);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
server_each_disconnect(void *elem, void *data)
{
    struct server *server;
    struct server_pool *pool;

    server = elem;
    pool = server->owner;

    while (!TAILQ_EMPTY(&server->s_conn_q)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q > 0);

        conn = TAILQ_FIRST(&server->s_conn_q);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

static void
server_failure(struct context *ctx, struct server *server)
{
    struct server_pool *pool = server->owner;
    int64_t now, next;
    rstatus_t status;

    if (!pool->auto_eject_hosts) {
        return;
    }

    server->failure_count++;

    log_debug(LOG_VERB, "server '%.*s' failure count %"PRIu32" limit %"PRIu32,
              server->pname.len, server->pname.data, server->failure_count,
              pool->server_failure_limit);

    if (server->failure_count < pool->server_failure_limit) {
        return;
    }

    now = nc_usec_now();
    if (now < 0) {
        return;
    }
    next = now + pool->server_retry_timeout;

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to delete server '%.*s' "
              "for next %"PRIu32" secs", pool->idx, pool->name.len,
              pool->name.data, server->pname.len, server->pname.data,
              pool->server_retry_timeout / 1000 / 1000);

    stats_pool_incr(ctx, pool, server_ejects);

    server->failure_count = 0;
    server->next_retry = next;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" '%.*s' failed: %s", pool->idx,
                  pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
server_close_stats(struct context *ctx, struct server *server, err_t err,
                   unsigned eof, unsigned connected)
{
    if (connected) {
        stats_server_decr(ctx, server, server_connections);
    }

    if (eof) {
        stats_server_incr(ctx, server, server_eof);
        return;
    }

    switch (err) {
    case ETIMEDOUT:
        stats_server_incr(ctx, server, server_timedout);
        break;
    case EPIPE:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
        stats_server_incr(ctx, server, server_err);
        break;
    }
}

void
server_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */
    struct conn *c_conn;    /* peer client connection */

    ASSERT(!conn->client && !conn->proxy);

    server_close_stats(ctx, conn->owner, conn->err, conn->eof,
                       conn->connected);

    if (conn->sd < 0) {
        server_failure(ctx, conn->owner);
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server inq */
        conn->dequeue_inq(ctx, conn, msg);

        /*
         * Don't send any error response, if
         * 1. request is tagged as noreply or,
         * 2. client has already closed its connection
         */
        if (msg->swallow || msg->noreply || msg->owner == NULL) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->swallow || msg->owner == NULL) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(!msg->request);
        ASSERT(msg->peer == NULL);

        rsp_put(msg);

        log_debug(LOG_INFO, "close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                  "in error", conn->sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    server_failure(ctx, conn->owner);

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

rstatus_t
server_connect(struct context *ctx, struct server *server, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->sd > 0) {
        /* already connected on server connection */
        return NC_OK;
    }

    log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
              server->pname.data);

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("socket for server '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(conn->sd);
    if (status != NC_OK) {
        log_error("set nonblock on s %d for server '%.*s' failed: %s",
                  conn->sd,  server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(conn->sd);
        if (status != NC_OK) {
            log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }

    status = event_add_conn(ctx->evb, conn);
    if (status != NC_OK) {
        log_error("event add conn s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
                      conn->sd, server->pname.len, server->pname.data);
            return NC_OK;
        }

        log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);

    return NC_OK;

error:
    conn->err = errno;
    return status;
}

void
server_connected(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connecting && !conn->connected);

    stats_server_incr(ctx, server, server_connections);

    conn->connecting = 0;
    conn->connected = 1;

    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);
}

void
server_ok(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
                  " to 0", server->pname.len, server->pname.data,
                  server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0LL;
    }
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
    rstatus_t status;
    int64_t now;
    uint32_t pnlive_server; /* prev # live server */

    if (!pool->auto_eject_hosts) {
        return NC_OK;
    }

    if (pool->next_rebuild == 0LL) {
        return NC_OK;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    if (now <= pool->next_rebuild) {
        if (pool->nlive_server == 0) {
            errno = ECONNREFUSED;
            return NC_ERROR;
        }
        return NC_OK;
    }

    pnlive_server = pool->nlive_server;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" with dist %d failed: %s", pool->idx,
                  pool->dist_type, strerror(errno));
        return status;
    }

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data,
              pool->nlive_server - pnlive_server);


    return NC_OK;
}

static uint32_t
server_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&pool->server) != 0);

    if (array_n(&pool->server) == 1) {
        return 0;
    }

    ASSERT(key != NULL && keylen != 0);

    return pool->key_hash((char *)key, keylen);
}

static struct server *
server_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    struct server *server;
    uint32_t hash, idx;

    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL && keylen != 0);

    switch (pool->dist_type) {
        case DIST_KETAMA:
            hash = server_pool_hash(pool, key, keylen);
            idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);
            break;

        case DIST_MODULA:
            hash = server_pool_hash(pool, key, keylen);
            idx = modula_dispatch(pool->continuum, pool->ncontinuum, hash);
            break;

        case DIST_RANDOM:
            idx = random_dispatch(pool->continuum, pool->ncontinuum, 0);
            break;
            
        case DIST_RANGE:
            hash = server_pool_hash(pool, key, keylen);
            hash ^= server_pool_hash(pool, pool->name.data, pool->name.len);
            idx = range_dispatch(pool->continuum, pool->ncontinuum, hash);
            break;

        default:
            NOT_REACHED();
            return NULL;
    }
    ASSERT(idx < array_n(&pool->server));

    server = array_get(&pool->server, idx);

    log_debug(LOG_VERB, "key '%.*s' on dist %d maps to server '%.*s'", keylen,
              key, pool->dist_type, server->pname.len, server->pname.data);

    return server;
}

struct conn *
server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
                 uint32_t keylen)
{
    rstatus_t status;
    struct server *server;
    struct conn *conn;

    status = server_pool_update(pool);
    if (status != NC_OK) {
        log_debug(LOG_VERB, "server: failed to update pool");
        return NULL;
    }

    /* from a given {key, keylen} pick a server from pool */
    server = server_pool_server(pool, key, keylen);
    if (server == NULL) {
        log_debug(LOG_VERB, "server: failed to pick server");
        return NULL;
    }

    /* pick a connection to a given server */
    conn = server_conn(server);
    if (conn == NULL) {
        log_debug(LOG_VERB, "server: failed to pick conn");
        return NULL;
    }

    status = server_connect(ctx, server, conn);
    if (status != NC_OK) {
        log_debug(LOG_VERB, "server: failed to connect");
        server_close(ctx, conn);
        return NULL;
    }

    return conn;
}

static rstatus_t
server_pool_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    if (!sp->preconnect) {
        return NC_OK;
    }

    status = array_each(&sp->server, server_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
server_pool_preconnect(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_disconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->server, server_each_disconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
server_pool_disconnect(struct context *ctx)
{
    array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static rstatus_t
server_pool_each_set_owner(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    sp->ctx = ctx;

    return NC_OK;
}

static rstatus_t
server_pool_each_set_gutter(void *elem, void *data)
{
    struct server_pool *sp = elem, *pool;
    struct array *pool_array = data;
    uint32_t pool_index;
    
    if (!string_empty(&sp->gutter_name)) {
        for (pool_index = 0; pool_index < array_n(pool_array); pool_index++) {
            pool = array_get(pool_array, pool_index);

            if (string_compare(&pool->name, &sp->gutter_name) == 0) {
                sp->gutter = pool;
                return NC_OK;
            }
        }
        return NC_ERROR;
    }
    
    return NC_OK;
}

static rstatus_t
server_pool_each_set_peer(void *elem, void *data)
{
    struct server_pool *sp = elem, *pool;
    struct array *pool_array = data;
    uint32_t pool_index;
    
    if (!string_empty(&sp->peer_name)) {
        for (pool_index = 0; pool_index < array_n(pool_array); pool_index++) {
            pool = array_get(pool_array, pool_index);

            if (string_compare(&pool->name, &sp->peer_name) == 0) {
                sp->peer = pool;
                return NC_OK;
            }
        }
        return NC_ERROR;
    }
    
    return NC_OK;
}


static rstatus_t
server_pool_each_set_downstreams(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp, *pool, *ds; /* downstream */
    struct array *pool_array;
    struct string *ds_name;
    uint32_t dsi, pi;            /* server_index and pool_index */
    
    sp = elem;
    pool_array = data;

    /* return if this is not a virtual server pool */
    if (!sp->virtual) {
        return NC_OK;
    }
    

    
    for (dsi = 0; dsi < array_n(&sp->downstream_names); dsi++) {
        ds_name = array_get(&sp->downstream_names, dsi);
        ds = NULL;
        
        /* find the coresponding server pool */
        for (pi = 0; pi < array_n(pool_array); pi++) {
            pool = array_get(pool_array, pi);
            if (!pool->virtual && 
                string_compare(&pool->name, ds_name) == 0) {
                ds = pool;
            }
        }

        if (ds) {
            if (string_empty(&ds->namespace)) {
                log_error("server: downstream '%.*s' has no namespace",
                          ds->name.len, ds->name.data);
                return NC_ERROR;
            } 

            if (sp->redis != ds->redis) {
                log_error("server: downstream '%.*s' has difference protocol type",
                          ds->name.len, ds->name.data);
                
                return NC_ERROR;
            }

            status = assoc_insert(sp->downstreams, 
                                  (const char *)ds->namespace.data, 
                                  ds->namespace.len, 
                                  ds);
            if (status != NC_OK) {
                log_error("server: failed to insert downstream");
                return status;
            }
            
            log_debug(LOG_VERB, "server: add downstream '%.*s' -> '%.*s'",
                      ds->namespace.len, ds->namespace.data,
                      ds->name.len, ds->name.data);
        } else {
            log_error("server: failed to find matching downstream");
            return NC_ERROR;
        }
    }

    return NC_OK;
}

rstatus_t
server_pool_run(struct server_pool *pool)
{
    ASSERT(pool->virtual || array_n(&pool->server) != 0);

    if (pool->virtual) {
        return NC_OK;
    }

    switch (pool->dist_type) {
        case DIST_KETAMA:
            return ketama_update(pool);

        case DIST_MODULA:
            return modula_update(pool);

        case DIST_RANDOM:
            return random_update(pool);

        case DIST_RANGE:
            return range_update(pool);
        default:
            NOT_REACHED();
            return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_run(void *elem, void *data)
{
    return server_pool_run(elem);
}

rstatus_t
server_pool_init(struct array *server_pool, struct array *conf_pool,
                 struct context *ctx)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(conf_pool);
    ASSERT(npool != 0);
    ASSERT(array_n(server_pool) == 0);

    status = array_init(server_pool, npool, sizeof(struct server_pool));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf pool to server pool */
    status = array_each(conf_pool, conf_pool_each_transform, server_pool);
    if (status != NC_OK) {
        log_error("server: failed to transform conf");
        server_pool_deinit(server_pool);
        return status;
    }
    ASSERT(array_n(server_pool) == npool);

    /* set ctx as the server pool owner */
    status = array_each(server_pool, server_pool_each_set_owner, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* set gutter pool for each server pool */
    status = array_each(server_pool, server_pool_each_set_gutter, server_pool);
    if (status != NC_OK) {
        log_error("server: failed to set gutter pool");
        server_pool_deinit(server_pool);
        return status;
    }

    /* set peer pool for each server pool */
    status = array_each(server_pool, server_pool_each_set_peer, server_pool);
    if (status != NC_OK) {
        log_error("server: failed to set peer pool");
        server_pool_deinit(server_pool);
        return status;
    }
    
    /* set downstream server pools for each server pool */
    status = array_each(server_pool, server_pool_each_set_downstreams, server_pool);
    if (status != NC_OK) {
        log_error("server: failed to set downstreams");
        server_pool_deinit(server_pool);
        return status;
    }

    /* update server pool continuum */
    status = array_each(server_pool, server_pool_each_run, NULL);
    if (status != NC_OK) {
        log_error("server: failed to run server");
        server_pool_deinit(server_pool);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" pools", npool);

    return NC_OK;
}

void
server_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

        if (sp->continuum != NULL) {
            nc_free(sp->continuum);
            sp->ncontinuum = 0;
            sp->nserver_continuum = 0;
            sp->nlive_server = 0;
        }
        
        array_deinit(&sp->downstream_names);

        if (sp->downstreams != NULL) {
            assoc_destroy_table(sp->downstreams);
        }

        server_deinit(&sp->server);

        log_debug(LOG_DEBUG, "deinit pool %"PRIu32" '%.*s'", sp->idx,
                  sp->name.len, sp->name.data);
    }

    array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" pools", npool);
}

/* NOTE: always return NC_OK */
static rstatus_t
server_each_probe(void *elem, void *data)
{
    rstatus_t status;
    struct server *server = elem;
    struct server_pool *pool = data;
    struct conn *conn;
    struct msg *msg;
    int64_t now;
    
    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    if (server->next_probe > now) {
        log_debug(LOG_VERB, "server: probe in %"PRIu64"s", 
                  (server->next_probe - now)/1000000);
        return NC_OK;
    }
    
    server->next_probe = now + pool->server_retry_timeout;

    status = server_pool_update(pool);
    if (status != NC_OK) {
        log_debug(LOG_VERB, "server: failed to update pool");
        return NC_OK;
    }

    conn = server_conn(server);
    if (conn == NULL) {
        log_debug(LOG_VERB, "server: failed to fetch conn");
        return NC_OK;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
        return NC_OK;
    }
    
    /* create a message probe message */
    msg = msg_build_probe(conn->redis);
    if (msg == NULL) {
        return NC_OK;
    }
    
    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = event_add_out(pool->ctx->evb, conn);
        if (status != NC_OK) {
            req_put(msg);
            conn->err = errno;
            return NC_OK;
        }
    }

    conn->enqueue_inq(pool->ctx, conn, msg);

    log_debug(LOG_VERB, "probe sent to %.*s", 
              server->pname.len, server->pname.data);
    
    return NC_OK;
}

static rstatus_t
server_pool_each_probe(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *pool = elem;
    struct array *servers;

    if (!pool->auto_probe_hosts) {
        return NC_OK;
    }

    servers = &pool->server;
    
    status = array_each(servers, server_each_probe, pool);
    
    /* always returns NC_OK */
    return NC_OK;
}


rstatus_t
server_pool_probe(struct context *ctx)
{
    rstatus_t status;
    struct array *pools;

    pools = &ctx->pool;

    status = array_each(pools, server_pool_each_probe, NULL);

    /* always returns NC_OK */
    return NC_OK;
}

static rstatus_t
server_pool_each_update_ratelimit(void *elem, void *data)
{
    struct server_pool *pool = elem;
    float delta;
    
    if (pool->rate != CONF_DEFAULT_RATE &&
        pool->burst != CONF_DEFAULT_BURST) {
        delta = pool->rate * NC_TICK_INTERVAL / 1000;
        if (pool->count >= delta) {
            pool->count -= delta;
        } else {
            pool->count = 0;
        }
    }

    log_debug(LOG_VERB, "ratelimit: %.*s rate: %.2f burst: %.2f count: %.2f",
              pool->name.len, pool->name.data,
              pool->rate, pool->burst, pool->count);

    return NC_OK;
}

void
server_pool_update_ratelimit(struct context *ctx)
{
    struct array *pools;

    pools = &ctx->pool;

    array_each(pools, server_pool_each_update_ratelimit, NULL);
}

bool
server_pool_ratelimit(struct server_pool *pool)
{
    if (pool->rate != CONF_DEFAULT_RATE && pool->burst != CONF_DEFAULT_BURST) {
        /* ratelimit enabled */
        if (pool->count < pool->burst) {
            pool->count += 1;
            return false;
        } else {
            return true;
        }
    } else {
        return false;
    }                
}


void 
server_warmup(struct msg *req, struct msg *rsp)
{
    rstatus_t status;
    struct conn *conn;
    struct server *server;
    struct server_pool *pool;
    struct msg *msg;

    conn = req->target;
    server = conn->owner;
    pool = server->owner;

    msg = msg_build_warmup(req, rsp);
    if (msg == NULL) {
        return;
    }

    ASSERT(msg->noreply);

    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = event_add_out(pool->ctx->evb, conn);
        if (status != NC_OK) {
            req_put(msg);
            conn->err = errno;
            log_warn("server: failed to add out event for warmup request");
            return;
        }
    }
    conn->enqueue_inq(pool->ctx, conn, msg);
    
    log_debug(LOG_VERB, "warmup sent to %.*s",
              server->pname.len, server->pname.data);
}
