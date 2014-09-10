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

#include <nc_core.h>
#include <nc_server.h>

struct msg *
req_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    msg = msg_get(conn, true, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
req_put(struct msg *msg)
{
    struct msg *pmsg; /* peer message (response) */

    ASSERT(msg->request);

    if (msg->pre_req_put != NULL) {
        msg->pre_req_put(msg);
    }

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    msg_tmo_delete(msg);

    msg_put(msg);
}

/*
 * Return true if request is done, false otherwise
 *
 * A request is done, if we received response for the given request.
 * A request vector is done if we received responses for all its
 * fragments.
 */
bool
req_done(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg, *pmsg; /* current and previous message */
    uint64_t id;             /* fragment id */
    uint32_t nfragment;      /* # fragment */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);

    if (!msg->done) {
        return false;
    }
    
    if (msg->waiting) {
        return false;
    }

    id = msg->frag_id;
    if (id == 0) {
        return true;
    }

    if (msg->fdone) {
        /* request has already been marked as done */
        return true;
    }

    /* check all fragments of the given request vector are done */

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    if (!pmsg->last_fragment) {
        return false;
    }

    /*
     * At this point, all the fragments including the last fragment have
     * been received.
     *
     * Mark all fragments of the given request vector to be done to speed up
     * future req_done calls for any of fragments of this request
     */

    msg->fdone = 1;
    nfragment = 1;

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    ASSERT(msg->frag_owner->nfrag == nfragment);

    msg->post_coalesce(msg->frag_owner);

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is done", conn->sd, id, nfragment);

    return true;
}

/*
 * Return true if request is in error, false otherwise
 *
 * A request is in error, if there was an error in receiving response for the
 * given request. A multiget request is in error if there was an error in
 * receiving response for any its fragments.
 */
bool
req_error(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg; /* current message */
    uint64_t id;
    uint32_t nfragment;

    ASSERT(msg->request && req_done(conn, msg));

    if (msg->error) {
        return true;
    }

    id = msg->frag_id;
    if (id == 0) {
        return false;
    }

    if (msg->ferror) {
        /* request has already been marked to be in error */
        return true;
    }

    /* check if any of the fragments of the given request are in error */

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    return false;

ferror:

    /*
     * Mark all fragments of the given request to be in error to speed up
     * future req_error calls for any of fragments of this request
     */

    msg->ferror = 1;
    nfragment = 1;

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is in error", conn->sd, id, nfragment);

    return true;
}

void
req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg != NULL);
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not intrested
     * in the reponse anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, in_queue);
    stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, in_queue);
    stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg != NULL);
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, out_queue);
    stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

void
req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, out_queue);
    stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(conn->client && !conn->proxy);

    if (conn->eof) {
        msg = conn->rmsg;

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put(msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn->active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}
static void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->client && !conn->proxy);

    log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
              "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
              nc_strerror(errno));

    msg->done = 1;
    msg->error = 1;
    msg->err = errno;

    /* noreply request don't expect any response */
    if (msg->noreply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
    }
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put(msg);
        return true;
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (msg->quit) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(msg);
        return true;
    }
    return false;
}


static void
req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);

    stats_server_incr(ctx, server, requests);
    stats_server_incr_by(ctx, server, request_bytes, msg->mlen);
}


static void
req_hash_key(const uint8_t *start, const uint8_t *end, const struct string *tag, struct string *key)
{
    uint8_t *tag_start, *tag_end;

    ASSERT(start != NULL && end > start && tag != NULL && key != NULL);
    
    tag_start = nc_strchr(start, end, tag->data[0]);
    if (tag_start != NULL) {
        tag_end = nc_strchr(tag_start + 1, end, tag->data[1]);
        if (tag_end != NULL) {
            key->data = tag_start + 1;
            key->len = (uint32_t)(tag_end - key->data);
        }
    }
}

rstatus_t
req_enqueue(struct context *ctx, struct conn *s_conn, struct msg *msg)
{
    rstatus_t status;

    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
        status = event_add_out(ctx->evb, s_conn);
        if (status != NC_OK) {
            s_conn->err = errno;
            return status;
        }
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);

    return NC_OK;
}

struct string
req_build_key(struct string *hash_tag, struct msg *msg)
{
    struct string key = null_string;
    /*
     * If hash_tag: is configured for this server pool, we use the part of
     * the key within the hash tag as an input to the distributor. Otherwise
     * we use the full key
     */
    if (!string_empty(hash_tag)) {
        req_hash_key(msg->key_start, msg->key_end, hash_tag, &key);
    } 

    /* Fallback to the whole string as hash_key */
    if (string_empty(&key)) {
        string_set(&key, msg->key_start, (msg->key_end - msg->key_start));
    }

    return key;
}

static rstatus_t
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    rstatus_t status;
    struct conn *s_conn; /* fallback connection */
    struct server_pool *pool;
    struct string key;

    ASSERT(c_conn->client && !c_conn->proxy);

    pool = c_conn->owner;
    key = req_build_key(&pool->hash_tag, msg);
    
    s_conn = msg->routing(ctx, pool, msg, &key);
    if (s_conn == NULL) {
        return NC_ERROR;
    }

    ASSERT(!s_conn->client && !s_conn->proxy);

    if (msg->post_routing != NULL &&
        msg->post_routing(ctx, s_conn, msg) != NC_OK) {
        return NC_ERROR;
    }

    /* enqueue the message (request) into server inq */
    status = req_enqueue(ctx, s_conn, msg);
    if (status != NC_OK) {
        return NC_ERROR;
    }

    req_forward_stats(ctx, s_conn->owner, msg);
    log_debug(LOG_VERB, "forward from c %d to s %d req %"PRIu64" len %"PRIu32
              " type %d with key '%.*s'", c_conn->sd, s_conn->sd, msg->id,
              msg->mlen, msg->type, key.len, key.data);
    return NC_OK;
}

static rstatus_t
req_virtual_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    rstatus_t status;
    struct server_pool *pool, *downstream;
    struct string namespace = null_string;

    pool = c_conn->owner;

    ASSERT(pool->virtual && !string_empty(&pool->hash_tag));

    req_hash_key(msg->key_start, msg->key_end, &pool->hash_tag, &namespace);
    if (string_empty(&namespace)) {
        log_debug(LOG_VERB, "no downstream namespace");
        errno = EINVAL;
        return NC_ERROR;
    }

    downstream = assoc_find(pool->downstream_table, 
                            (const char *)namespace.data, namespace.len);
    if (downstream == NULL) {
        log_warn("no downstream for namespace \"%.*s\"", namespace.len, namespace.data);
        return NC_ERROR;
    }
    log_debug(LOG_VERB, "forward to downstream: '%.*s' -> '%.*s'", 
              downstream->namespace.len, downstream->namespace.data,
              downstream->name.len, downstream->name.data);
    /* Transfer this connection to the downstream pool */
    c_conn->unref(c_conn);
    c_conn->ref(c_conn, downstream);
    
    status = req_forward(ctx, c_conn, msg);
    if (status != NC_OK) {
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
req_pre_forward(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct server_pool *pool;

    pool = conn->owner;

    if (server_pool_ratelimit(pool)) {
        errno = NC_ETOOMANYREQUESTS;
        return NC_ERROR;
    }

    if (msg->pre_req_forward != NULL &&
        msg->pre_req_forward(ctx, conn, msg) != NC_OK) {
        return NC_ERROR;
    }

    return NC_OK;
}

void
req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    rstatus_t status;
    struct server_pool *pool;

    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    pool = conn->owner;

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (req_filter(ctx, conn, msg)) {
        return;
    }

    /* enqueue message (request) into client outq, if response is expected */
    if (!msg->noreply) {
        conn->enqueue_outq(ctx, conn, msg);
    }

    status = req_pre_forward(ctx, conn, msg);
    if (status != NC_OK) {
        req_forward_error(ctx, conn, msg);
        return;
    }

    if (pool->virtual) {
        status = req_virtual_forward(ctx, conn, msg);
    } else {
        status = req_forward(ctx, conn, msg);
    }
    if (status != NC_OK) {
        req_forward_error(ctx, conn, msg);
    }
}

struct msg *
req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(!conn->client && !conn->proxy);

    if (conn->connecting) {
        server_connected(ctx, conn);
    }

    nmsg = TAILQ_FIRST(&conn->imsg_q);
    if (nmsg == NULL) {
        /* nothing to send as the server inq is empty */
        status = event_del_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(msg->request && !msg->done);
        nmsg = TAILQ_NEXT(msg, s_tqe);
    }

    conn->smsg = nmsg;

    if (nmsg == NULL) {
        return NULL;
    }

    ASSERT(nmsg->request && !nmsg->done);

    log_debug(LOG_VVERB, "send next req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", nmsg->id, nmsg->mlen, nmsg->type, conn->sd);

    return nmsg;
}

void
req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(msg != NULL && conn->smsg == NULL);
    ASSERT(msg->request && !msg->done);
    ASSERT(msg->owner != conn);

    log_debug(LOG_VVERB, "send done req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", msg->id, msg->mlen, msg->type, conn->sd);

    /* dequeue the message (request) from server inq */
    conn->dequeue_inq(ctx, conn, msg);

    /*
     * noreply request instructs the server not to send any response. So,
     * enqueue message (request) in server outq, if response is expected.
     * Otherwise, free the noreply request
     */
    if (!msg->noreply) {
        conn->enqueue_outq(ctx, conn, msg);
    } else {
        req_put(msg);
    }
}
