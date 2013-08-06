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

#include <ctype.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_proto.h>

/*
 * From memcache protocol specification:
 *
 * Data stored by memcached is identified with the help of a key. A key
 * is a text string which should uniquely identify the data for clients
 * that are interested in storing and retrieving it.  Currently the
 * length limit of a key is set at 250 characters (of course, normally
 * clients wouldn't need to use such long keys); the key must not include
 * control characters or whitespace.
 */
#define MEMCACHE_MAX_KEY_LENGTH 250

#define MEMCACHE_PROBE_MESSAGE "stats\r\n"

#define STATS_OK (void *) NULL

struct memcache_stats {
    uint32_t uptime;
    uint32_t cold;
    uint64_t cmd_get;
    uint64_t get_hits;
};


struct stats_command {
    struct string name;
    char *(*set)(struct memcache_stats *s, struct stats_command *cmd, void *val);
    int offset;
};

static char *
stats_set_uint(struct memcache_stats *s, struct stats_command *cmd, void *val)
{
    uint8_t *p;
    int num;
    uint32_t *np;
    struct string *value = val;
    
    p = (uint8_t *)s;
    np = (uint32_t *)(p + cmd->offset);
    
    num = nc_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;
    return STATS_OK;
}

static char *
stats_set_ulong(struct memcache_stats *s, struct stats_command *cmd, void *val)
{
    uint8_t *p;
    int num;   
    uint64_t *np;
    struct string *value = val;
    
    p = (uint8_t *)s;
    np = (uint64_t *)(p + cmd->offset);
    
    /* FIXME: long integer overflow */
    num = nc_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;
    return STATS_OK;
}

#define null_stats_command { null_string, NULL, 0 }

static struct stats_command commands[] = {
    { string("uptime"),
      stats_set_uint,
      offsetof(struct memcache_stats, uptime) },

    { string("cold"),
      stats_set_uint,
      offsetof(struct memcache_stats, cold) },

    { string("cmd_get"),
      stats_set_ulong,
      offsetof(struct memcache_stats, cmd_get) },
    
    { string("get_hits"),
      stats_set_ulong,
      offsetof(struct memcache_stats, get_hits) },
    
    null_stats_command
};


static rstatus_t
memcache_update_stat(struct memcache_stats *s, struct string *k, struct string *v)
{
    struct stats_command *cmd;

    ASSERT(s != NULL);
    
    for (cmd = commands; cmd->name.len != 0; cmd++) {
        char *rv;

        if (string_compare(k, &cmd->name) != 0) {
            continue;
        }
        
        rv = cmd->set(s, cmd, v);
                
        if (rv != STATS_OK) {
            log_warn("stats: \"%.*s\" %s", k->len, k->data, rv);
            return NC_ERROR;
        }
        
        return NC_OK;
    }

    return NC_OK;
}

/*
 * Return true, if the memcache command is a storage command, otherwise
 * return false
 */
static bool
memcache_storage(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_SET:
    case MSG_REQ_MC_CAS:
    case MSG_REQ_MC_ADD:
    case MSG_REQ_MC_REPLACE:
    case MSG_REQ_MC_APPEND:
    case MSG_REQ_MC_PREPEND:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the memcache command is a cas command, otherwise
 * return false
 */
static bool
memcache_cas(struct msg *r)
{
    if (r->type == MSG_REQ_MC_CAS) {
        return true;
    }

    return false;
}

/*
 * Return true, if the memcache command is a retrieval command, otherwise
 * return false
 */
static bool
memcache_retrieval(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_GET:
    case MSG_REQ_MC_GETS:
        return true;

    default:
        break;
    }

    return false;
}

/* 
 * Return true, if the memcache command is a get command, otherwise 
 * return false
 */
static bool
memcache_get(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_GET:
        return true;
    default:
        break;
    }
    
    return false;
}

/* 
 * Return true, if the memcache response is a value, otherwise
 * return false
 */

static bool
memcache_value(struct msg *r)
{
    switch (r->type) {
    case MSG_RSP_MC_VALUE:
        return true;
    default:
        break;
    }
    
    return false;
}

/*
 * Return true, if the memcache command is a arithmetic command, otherwise
 * return false
 */
static bool
memcache_arithmetic(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_MC_INCR:
    case MSG_REQ_MC_DECR:
        return true;

    default:
        break;
    }

    return false;
}

/*
 * Return true, if the memcache command is a delete command, otherwise
 * return false
 */
static bool
memcache_delete(struct msg *r)
{
    if (r->type == MSG_REQ_MC_DELETE) {
        return true;
    }

    return false;
}

void
memcache_parse_req(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    enum {
        SW_START,
        SW_REQ_TYPE,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_KEYS,
        SW_SPACES_BEFORE_FLAGS,
        SW_FLAGS,
        SW_SPACES_BEFORE_EXPIRY,
        SW_EXPIRY,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_SPACES_BEFORE_CAS,
        SW_CAS,
        SW_RUNTO_VAL,
        SW_VAL,
        SW_SPACES_BEFORE_NUM,
        SW_NUM,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_NOREPLY,
        SW_AFTER_NOREPLY,
        SW_ALMOST_DONE,
        SW_SENTINEL
    } state;

    state = r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    ASSERT(r->request);
    ASSERT(!r->redis);
    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(b != NULL);
    ASSERT(b->pos <= b->last);

    /* validate the parsing maker */
    ASSERT(r->pos != NULL);
    ASSERT(r->pos >= b->pos && r->pos <= b->last);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {

        case SW_START:
            if (ch == ' ') {
                break;
            }

            if (!islower(ch)) {
                goto error;
            }

            /* req_start <- p; type_start <- p */
            r->token = p;
            state = SW_REQ_TYPE;

            break;

        case SW_REQ_TYPE:
            if (ch == ' ' || ch == CR) {
                /* type_end = p - 1 */
                m = r->token;
                r->token = NULL;
                r->type = MSG_UNKNOWN;

                switch (p - m) {

                case 3:
                    if (str4cmp(m, 'g', 'e', 't', ' ')) {
                        r->type = MSG_REQ_MC_GET;
                        break;
                    }

                    if (str4cmp(m, 's', 'e', 't', ' ')) {
                        r->type = MSG_REQ_MC_SET;
                        break;
                    }

                    if (str4cmp(m, 'a', 'd', 'd', ' ')) {
                        r->type = MSG_REQ_MC_ADD;
                        break;
                    }

                    if (str4cmp(m, 'c', 'a', 's', ' ')) {
                        r->type = MSG_REQ_MC_CAS;
                        break;
                    }

                    break;

                case 4:
                    if (str4cmp(m, 'g', 'e', 't', 's')) {
                        r->type = MSG_REQ_MC_GETS;
                        break;
                    }

                    if (str4cmp(m, 'i', 'n', 'c', 'r')) {
                        r->type = MSG_REQ_MC_INCR;
                        break;
                    }

                    if (str4cmp(m, 'd', 'e', 'c', 'r')) {
                        r->type = MSG_REQ_MC_DECR;
                        break;
                    }

                    if (str4cmp(m, 'q', 'u', 'i', 't')) {
                        r->type = MSG_REQ_MC_QUIT;
                        r->quit = 1;
                        break;
                    }

                    break;

                case 6:
                    if (str6cmp(m, 'a', 'p', 'p', 'e', 'n', 'd')) {
                        r->type = MSG_REQ_MC_APPEND;
                        break;
                    }

                    if (str6cmp(m, 'd', 'e', 'l', 'e', 't', 'e')) {
                        r->type = MSG_REQ_MC_DELETE;
                        break;
                    }

                    break;

                case 7:
                    if (str7cmp(m, 'p', 'r', 'e', 'p', 'e', 'n', 'd')) {
                        r->type = MSG_REQ_MC_PREPEND;
                        break;
                    }

                    if (str7cmp(m, 'r', 'e', 'p', 'l', 'a', 'c', 'e')) {
                        r->type = MSG_REQ_MC_REPLACE;
                        break;
                    }

                    break;
                }

                switch (r->type) {
                case MSG_REQ_MC_GET:
                case MSG_REQ_MC_GETS:
                case MSG_REQ_MC_DELETE:
                case MSG_REQ_MC_CAS:
                case MSG_REQ_MC_SET:
                case MSG_REQ_MC_ADD:
                case MSG_REQ_MC_REPLACE:
                case MSG_REQ_MC_APPEND:
                case MSG_REQ_MC_PREPEND:
                case MSG_REQ_MC_INCR:
                case MSG_REQ_MC_DECR:
                    if (ch == CR) {
                        goto error;
                    }
                    state = SW_SPACES_BEFORE_KEY;
                    break;

                case MSG_REQ_MC_QUIT:
                    p = p - 1; /* go back by 1 byte */
                    state = SW_CRLF;
                    break;

                case MSG_UNKNOWN:
                    goto error;

                default:
                    NOT_REACHED();
                }

            } else if (!islower(ch)) {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                r->token = p;
                r->key_start = p;
                state = SW_KEY;
            }

            break;

        case SW_KEY:
            if (ch == ' ' || ch == CR) {
                if ((p - r->key_start) > MEMCACHE_MAX_KEY_LENGTH) {
                    log_error("parsed bad req %"PRIu64" of type %d with key "
                              "prefix '%.*s...' and length %d that exceeds "
                              "maximum key length", r->id, r->type, 16,
                              r->key_start, p - r->key_start);
                    goto error;
                }
                r->key_end = p;
                r->token = NULL;

                /* get next state */
                if (memcache_storage(r)) {
                    state = SW_SPACES_BEFORE_FLAGS;
                } else if (memcache_arithmetic(r)) {
                    state = SW_SPACES_BEFORE_NUM;
                } else if (memcache_delete(r)) {
                    state = SW_RUNTO_CRLF;
                } else if (memcache_retrieval(r)) {
                    state = SW_SPACES_BEFORE_KEYS;
                } else {
                    state = SW_RUNTO_CRLF;
                }

                if (ch == CR) {
                    if (memcache_storage(r) || memcache_arithmetic(r)) {
                        goto error;
                    }
                    p = p - 1; /* go back by 1 byte */
                }
            }

            break;

        case SW_SPACES_BEFORE_KEYS:
            ASSERT(memcache_retrieval(r));
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                r->token = p;
                goto fragment;
            }

            break;

        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* flags_start <- p; flags <- ch - '0' */
                r->token = p;
                state = SW_FLAGS;
            }

            break;

        case SW_FLAGS:
            if (isdigit(ch)) {
                /* flags <- flags * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* flags_end <- p - 1 */
                r->token = NULL;
                state = SW_SPACES_BEFORE_EXPIRY;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_EXPIRY:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* expiry_start <- p; expiry <- ch - '0' */
                r->token = p;
                state = SW_EXPIRY;
            }

            break;

        case SW_EXPIRY:
            if (isdigit(ch)) {
                /* expiry <- expiry * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* expiry_end <- p - 1 */
                r->token = NULL;
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* vlen_start <- p */
                r->token = p;
                r->vlen = (uint32_t)(ch - '0');
                state = SW_VLEN;
            }

            break;

        case SW_VLEN:
            if (isdigit(ch)) {
                r->vlen = r->vlen * 10 + (uint32_t)(ch - '0');
            } else if (memcache_cas(r)) {
                if (ch != ' ') {
                    goto error;
                }
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_SPACES_BEFORE_CAS;
            } else if (ch == ' ' || ch == CR) {
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_CAS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* cas_start <- p; cas <- ch - '0' */
                r->token = p;
                state = SW_CAS;
            }

            break;

        case SW_CAS:
            if (isdigit(ch)) {
                /* cas <- cas * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                /* cas_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;


        case SW_RUNTO_VAL:
            switch (ch) {
            case LF:
                /* val_start <- p + 1 */
                state = SW_VAL;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            m = p + r->vlen;
            if (m >= b->last) {
                ASSERT(r->vlen >= (uint32_t)(b->last - p));
                r->vlen -= (uint32_t)(b->last - p);
                m = b->last - 1;
                p = m; /* move forward by vlen bytes */
                break;
            }
            switch (*m) {
            case CR:
                /* val_end <- p - 1 */
                p = m; /* move forward by vlen bytes */
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_NUM:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                /* num_start <- p; num <- ch - '0'  */
                r->token = p;
                state = SW_NUM;
            }

            break;

        case SW_NUM:
            if (isdigit(ch)) {
                /* num <- num * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                r->token = NULL;
                /* num_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case ' ':
                break;

            case 'n':
                if (memcache_storage(r) || memcache_arithmetic(r) || memcache_delete(r)) {
                    /* noreply_start <- p */
                    r->token = p;
                    state = SW_NOREPLY;
                } else {
                    goto error;
                }

                break;

            case CR:
                if (memcache_storage(r)) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_NOREPLY:
            switch (ch) {
            case ' ':
            case CR:
                m = r->token;
                if (((p - m) == 7) && str7cmp(m, 'n', 'o', 'r', 'e', 'p', 'l', 'y')) {
                    ASSERT(memcache_storage(r) || memcache_arithmetic(r) || memcache_delete(r));
                    r->token = NULL;
                    /* noreply_end <- p - 1 */
                    r->noreply = 1;
                    state = SW_AFTER_NOREPLY;
                    p = p - 1; /* go back by 1 byte */
                } else {
                    goto error;
                }
            }

            break;

        case SW_AFTER_NOREPLY:
            switch (ch) {
            case ' ':
                break;

            case CR:
                if (memcache_storage(r)) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }
                break;

            default:
                goto error;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                /* req_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    /*
     * At this point, buffer from b->pos to b->last has been parsed completely
     * but we haven't been able to reach to any conclusion. Normally, this
     * means that we have to parse again starting from the state we are in
     * after more data has been read. The newly read data is either read into
     * a new mbuf, if existing mbuf is full (b->last == b->end) or into the
     * existing mbuf.
     *
     * The only exception to this is when the existing mbuf is full (b->last
     * is at b->end) and token marker is set, which means that we have to
     * copy the partial token into a new mbuf and parse again with more data
     * read into new mbuf.
     */
    ASSERT(p == b->last);
    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        r->pos = r->token;
        r->token = NULL;
        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

fragment:
    ASSERT(p != b->last);
    ASSERT(r->token != NULL);
    r->pos = r->token;
    r->token = NULL;
    r->state = state;
    r->result = MSG_PARSE_FRAGMENT;

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    ASSERT(r->pos <= b->last);
    r->state = SW_START;
    r->result = MSG_PARSE_OK;

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed req %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad req %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
}

void
memcache_parse_rsp(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    struct string *key, *val;
    enum {
        SW_START,
        SW_RSP_NUM,
        SW_RSP_STR,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_FLAGS,
        SW_FLAGS,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_RUNTO_VAL,
        SW_VAL,
        SW_VAL_LF,
        SW_END,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_ALMOST_DONE,
        SW_SPACES_BEFORE_INLINE_VAL,        
        SW_INLINE_VAL,
        SW_SENTINEL
    } state;

    state = r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    ASSERT(!r->request);
    ASSERT(!r->redis);
    ASSERT(state >= SW_START && state < SW_SENTINEL);
    ASSERT(b != NULL);
    ASSERT(b->pos <= b->last);

    /* validate the parsing marker */
    ASSERT(r->pos != NULL);
    ASSERT(r->pos >= b->pos && r->pos <= b->last);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {
        case SW_START:
            if (isdigit(ch)) {
                state = SW_RSP_NUM;
            } else {
                state = SW_RSP_STR;
            }
            p = p - 1; /* go back by 1 byte */

            break;

        case SW_RSP_NUM:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (isdigit(ch)) {
                /* num <- num * 10 + (ch - '0') */
                ;
            } else if (ch == ' ' || ch == CR) {
                /* type_end <- p - 1 */
                r->token = NULL;
                r->type = MSG_RSP_MC_NUM;
                p = p - 1; /* go back by 1 byte */
                state = SW_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RSP_STR:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (ch == ' ' || ch == CR) {
                /* type_end <- p - 1 */
                m = r->token;
                r->token = NULL;
                r->type = MSG_UNKNOWN;

                switch (p - m) {
                case 3:
                    if (str4cmp(m, 'E', 'N', 'D', '\r')) {
                        r->type = MSG_RSP_MC_END;
                        /* end_start <- m; end_end <- p - 1*/
                        r->end = m;
                        break;
                    }

                    break;
                    
                case 4:
                    if (str4cmp(m, 'S', 'T', 'A', 'T')) {
                        r->type = MSG_RSP_MC_STATS;
                        break;
                    }
                    
                    break;
                    
                case 5:
                    if (str5cmp(m, 'V', 'A', 'L', 'U', 'E')) {
                        /*
                         * Encompasses responses for 'get', 'gets' and
                         * 'cas' command.
                         */
                        r->type = MSG_RSP_MC_VALUE;
                        break;
                    }

                    if (str5cmp(m, 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_ERROR;
                        break;
                    }

                    break;

                case 6:
                    if (str6cmp(m, 'S', 'T', 'O', 'R', 'E', 'D')) {
                        r->type = MSG_RSP_MC_STORED;
                        break;
                    }

                    if (str6cmp(m, 'E', 'X', 'I', 'S', 'T', 'S')) {
                        r->type = MSG_RSP_MC_EXISTS;
                        break;
                    }

                    break;

                case 7:
                    if (str7cmp(m, 'D', 'E', 'L', 'E', 'T', 'E', 'D')) {
                        r->type = MSG_RSP_MC_DELETED;
                        break;
                    }

                    break;

                case 9:
                    if (str9cmp(m, 'N', 'O', 'T', '_', 'F', 'O', 'U', 'N', 'D')) {
                        r->type = MSG_RSP_MC_NOT_FOUND;
                        break;
                    }

                    break;

                case 10:
                    if (str10cmp(m, 'N', 'O', 'T', '_', 'S', 'T', 'O', 'R', 'E', 'D')) {
                        r->type = MSG_RSP_MC_NOT_STORED;
                        break;
                    }

                    break;

                case 12:
                    if (str12cmp(m, 'C', 'L', 'I', 'E', 'N', 'T', '_', 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_CLIENT_ERROR;
                        break;
                    }

                    if (str12cmp(m, 'S', 'E', 'R', 'V', 'E', 'R', '_', 'E', 'R', 'R', 'O', 'R')) {
                        r->type = MSG_RSP_MC_SERVER_ERROR;
                        break;
                    }

                    break;
                }

                switch (r->type) {
                case MSG_UNKNOWN:
                    goto error;

                case MSG_RSP_MC_STORED:
                case MSG_RSP_MC_NOT_STORED:
                case MSG_RSP_MC_EXISTS:
                case MSG_RSP_MC_NOT_FOUND:
                case MSG_RSP_MC_DELETED:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_END:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_VALUE:
                case MSG_RSP_MC_STATS:
                    state = SW_SPACES_BEFORE_KEY;
                    break;

                case MSG_RSP_MC_ERROR:
                    state = SW_CRLF;
                    break;

                case MSG_RSP_MC_CLIENT_ERROR:
                case MSG_RSP_MC_SERVER_ERROR:
                    state = SW_RUNTO_CRLF;
                    break;

                default:
                    NOT_REACHED();
                }

                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                state = SW_KEY;
                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_KEY:
            if (r->token == NULL) {
                r->token = p;
                r->key_start = p;
            }

            if (ch == ' ') {
                if ((p - r->key_start) > MEMCACHE_MAX_KEY_LENGTH) {
                    log_error("parsed bad req %"PRIu64" of type %d with key "
                              "prefix '%.*s...' and length %d that exceeds "
                              "maximum key length", r->id, r->type, 16,
                              r->key_start, p - r->key_start);
                    goto error;
                }
                r->key_end = p;
                r->token = NULL;
                if (r->type != MSG_RSP_MC_STATS) {
                    state = SW_SPACES_BEFORE_FLAGS;
                } else {
                    key = array_push(&r->keys);
                    key->len = r->key_end - r->key_start;
                    key->data = r->key_start;

                    state = SW_SPACES_BEFORE_INLINE_VAL;
                }
            }

            break;
        case SW_INLINE_VAL:
            if (r->token == NULL) {
                r->token = p;
                r->val_start = p;
            }
            
            if (ch == CR) {
                r->val_end = p;
                r->token = NULL;

                val = array_push(&r->vals);
                val->len = r->val_end - r->val_start;
                val->data = r->val_start;

                state = SW_VAL_LF;
            }
            
            break;
        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                state = SW_FLAGS;
                p = p - 1; /* go back by 1 byte */
            }

            break;
            
        case SW_SPACES_BEFORE_INLINE_VAL:
            if (ch != ' ') {
                state = SW_INLINE_VAL;
                p = p - 1;
            }
            
            break;

        case SW_FLAGS:
            if (r->token == NULL) {
                /* flags_start <- p */
                r->token = p;
                r->flags_start = p;
            }

            if (isdigit(ch)) {
                /* flags <- flags * 10 + (ch - '0') */
                ;
            } else if (ch == ' ') {
                /* flags_end <- p - 1 */
                r->flags_end = p;
                r->token = NULL;
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1; /* go back by 1 byte */
                state = SW_VLEN;
            }

            break;

        case SW_VLEN:
            if (r->token == NULL) {
                /* vlen_start <- p */
                r->token = p;
                r->vlen = (uint32_t)(ch - '0');
            } else if (isdigit(ch)) {
                r->vlen = r->vlen * 10 + (uint32_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_VAL:
            switch (ch) {
            case LF:
                /* val_start <- p + 1 */
                state = SW_VAL;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            /* FIXME: maybe we can unify the INLINE_VAL and VAL? */
            if (r->val_start == NULL) {
                r->val_start = p;
            }
            m = p + r->vlen;
            if (m >= b->last) {
                ASSERT(r->vlen >= (uint32_t)(b->last - p));
                r->vlen -= (uint32_t)(b->last - p);
                m = b->last - 1;
                p = m; /* move forward by vlen bytes */
                break;
            }
            switch (*m) {
            case CR:
                /* val_end <- p - 1 */
                r->val_end = p - 1;
                p = m; /* move forward by vlen bytes */
                state = SW_VAL_LF;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL_LF:
            switch (ch) {
            case LF:
                if (r->type != MSG_RSP_MC_STATS) {
                    state = SW_END;
                } else {
                    state = SW_RSP_STR;
                }
                break;

            default:
                goto error;
            }

            break;

        case SW_END:
            if (r->token == NULL) {
                if (ch != 'E') {
                    goto error;
                }
                /* end_start <- p */
                r->token = p;
            } else if (ch == CR) {
                /* end_end <- p */
                m = r->token;
                r->token = NULL;

                switch (p - m) {
                case 3:
                    if (str4cmp(m, 'E', 'N', 'D', '\r')) {
                        r->end = m;
                        state = SW_ALMOST_DONE;
                    }
                    break;

                default:
                    goto error;
                }
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case CR:
                if (r->type == MSG_RSP_MC_VALUE) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                break;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                /* rsp_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    ASSERT(p == b->last);
    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        r->pos = r->token;
        r->token = NULL;
        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed rsp %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    ASSERT(r->pos <= b->last);
    r->state = SW_START;
    r->token = NULL;
    r->result = MSG_PARSE_OK;

    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "parsed rsp %"PRIu64" res %d "
                "type %d state %d rpos %d of %d", r->id, r->result, r->type,
                r->state, r->pos - b->pos, b->last - b->pos);
    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;

    log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad rsp %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
}

/*
 * Pre-split copy handler invoked when the request is a multi vector -
 * 'get' or 'gets' request and is about to be split into two requests
 */
void
memcache_pre_splitcopy(struct mbuf *mbuf, void *arg)
{
    struct msg *r = arg;                  /* request vector */
    struct string get = string("get ");   /* 'get ' string */
    struct string gets = string("gets "); /* 'gets ' string */

    ASSERT(r->request);
    ASSERT(!r->redis);
    ASSERT(mbuf_empty(mbuf));

    switch (r->type) {
    case MSG_REQ_MC_GET:
        mbuf_copy(mbuf, get.data, get.len);
        break;

    case MSG_REQ_MC_GETS:
        mbuf_copy(mbuf, gets.data, gets.len);
        break;

    default:
        NOT_REACHED();
    }
}

/*
 * Post-split copy handler invoked when the request is a multi vector -
 * 'get' or 'gets' request and has already been split into two requests
 */
rstatus_t
memcache_post_splitcopy(struct msg *r)
{
    struct mbuf *mbuf;
    struct string crlf = string(CRLF);

    ASSERT(r->request);
    ASSERT(!r->redis);
    ASSERT(!STAILQ_EMPTY(&r->mhdr));

    mbuf = STAILQ_LAST(&r->mhdr, mbuf, next);
    mbuf_copy(mbuf, crlf.data, crlf.len);

    return NC_OK;
}

/*
 * Pre-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'get' or 'gets' and all the
 * responses to the fragmented request vector hasn't been received
 */
void
memcache_pre_coalesce(struct msg *r)
{
    struct msg *pr = r->peer; /* peer request */
    struct mbuf *mbuf;

    ASSERT(!r->request);
    ASSERT(pr->request);

    if (pr->frag_id == 0) {
        /* do nothing, if not a response to a fragmented request */
        return;
    }

    switch (r->type) {

    case MSG_RSP_MC_VALUE:
    case MSG_RSP_MC_END:

        /*
         * Readjust responses of the fragmented message vector by not
         * including the end marker for all but the last response
         */

        if (pr->last_fragment) {
            break;
        }

        ASSERT(r->end != NULL);

        for (;;) {
            mbuf = STAILQ_LAST(&r->mhdr, mbuf, next);
            ASSERT(mbuf != NULL);

            /*
             * We cannot assert that end marker points to the last mbuf
             * Consider a scenario where end marker points to the
             * penultimate mbuf and the last mbuf only contains spaces
             * and CRLF: mhdr -> [...END] -> [\r\n]
             */

            if (r->end >= mbuf->pos && r->end < mbuf->last) {
                /* end marker is within this mbuf */
                r->mlen -= (uint32_t)(mbuf->last - r->end);
                mbuf->last = r->end;
                break;
            }

            /* end marker is not in this mbuf */
            r->mlen -= mbuf_length(mbuf);
            mbuf_remove(&r->mhdr, mbuf);
            mbuf_put(mbuf);
        }

        break;

    default:
        /*
         * Valid responses for a fragmented requests are MSG_RSP_MC_VALUE or,
         * MSG_RSP_MC_END. For an invalid response, we send out SERVER_ERRROR
         * with EINVAL errno
         */
        mbuf = STAILQ_FIRST(&r->mhdr);
        log_hexdump(LOG_ERR, mbuf->pos, mbuf_length(mbuf), "rsp fragment "
                    "with unknown type %d", r->type);
        pr->error = 1;
        pr->err = EINVAL;
        break;
    }
}

/*
 * Post-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'get' or 'gets' and all the
 * responses to the fragmented request vector has been received and
 * the fragmented request is consider to be done
 */
void
memcache_post_coalesce(struct msg *r)
{
}

rstatus_t
memcache_build_probe(struct msg *r)
{
    struct mbuf *mbuf;
    size_t msize, msglen;

    ASSERT(STAILQ_LAST(&r->mhdr, mbuf, next) == NULL);
    
    mbuf = mbuf_get();
    if (mbuf == NULL) {
        return NC_ENOMEM;
    }
    mbuf_insert(&r->mhdr, mbuf);
    r->pos = mbuf->pos;

    msize = mbuf_size(mbuf);
    msglen = sizeof(MEMCACHE_PROBE_MESSAGE) - 1;
    
    ASSERT(msize >= msglen);
    
    mbuf_copy(mbuf, (uint8_t *)MEMCACHE_PROBE_MESSAGE, msglen);
    r->mlen += (uint32_t)msglen;
    
    return NC_OK;
}

static void
memcache_handle_probe(struct msg *req, struct msg *rsp)
{
    rstatus_t status;
    struct string *key, *val;
    struct array *keys, *vals;
    struct server *server;
    struct memcache_stats *stats;
    uint32_t i, nkey;

    ASSERT(array_n(&rsp->keys) == array_n(&rsp->vals));
    ASSERT(rsp->owner->owner != NULL);
    
    keys = &rsp->keys;
    vals = &rsp->vals;
    nkey = array_n(&rsp->keys);
    server = rsp->owner->owner;
    stats = server->stats;

    for (i = 0; i < nkey; i++) {
        key = array_get(keys, i);
        val = array_get(vals, i);
        status = memcache_update_stat(stats, key, val);
    }
}       

struct memcache_stats *
memcache_create_stats()
{
    struct memcache_stats *stats;

    stats = nc_zalloc(sizeof(*stats));
    if (stats == NULL) {
        return NULL;
    }

    return stats;
}

void
memcache_destroy_stats(struct memcache_stats *stats)
{
    nc_free(stats);
}

static bool
memcache_cold(struct conn *conn)
{
    struct server *server;
    struct memcache_stats *stats;
    
    server = conn->owner;
    stats = server->stats;
    
    return stats->cold == 1;
}

static bool
memcache_need_warmup(struct msg *req, struct msg *rsp)
{
    /* Only handle get */
    if (memcache_get(req) && memcache_value(rsp)) {
        return true;
    }

    return false;
}

/* 
   Request:
   get <key>\r\n

   Response:
   VALUE <key> <flags> <bytes> [<cas unique>]\r\n
   <data block>\r\n
   END\r\n

   Warmup request:
   set <key> <flags> <exptime> <bytes> noreply\r\n
   <data block>\r\n
 */
static struct msg *
memcache_build_warmup(struct msg *req, struct msg *rsp)
{
    struct msg *msg;
    struct conn *conn;
    struct mbuf *src, *dst;
    int n;
    uint32_t remain, length, msize;
    uint8_t *pos;
    
    ASSERT(rsp->key_start && rsp->key_end);
    ASSERT(rsp->flags_start && rsp->flags_end);
    ASSERT(rsp->vlen > 0);
    
    conn = req->owner;

    msg = msg_get(NULL, true, conn->redis);
    if (msg == NULL) {
        return NULL;
    }

    dst = mbuf_get();
    if (dst == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, dst);

    msize = mbuf_size(dst);
    /* Write command line */
    n = nc_scnprintf(dst->last, msize, "set %.*s %.*s %d %d noreply\r\n",
                     (int)(rsp->key_end - rsp->key_start), rsp->key_start,
                     (int)(rsp->flags_end - rsp->flags_start), rsp->flags_start,
                     0,
                     rsp->vlen);
    dst->last += n;
    ASSERT(dst->last <= dst->end);
    
    remain = rsp->vlen + 2;     /* <data block>\r\n */
    STAILQ_FOREACH(src, &rsp->mhdr, next) {
        /* Copy value from each source mbuf */
        if (rsp->val_start >= src->pos && rsp->val_start < src->last) {
            pos = rsp->val_start;
        } else {
            pos = src->pos;
        }
        length = (uint32_t)(src->last - pos);
        while (length > 0 && remain > 0) {
            if (mbuf_full(dst)) {
                dst = mbuf_get();
                if (dst == NULL) {
                    msg_put(msg);
                    return NULL;
                }
                mbuf_insert(&msg->mhdr, dst);
            }

            n = MIN(MIN(length, mbuf_size(dst)), remain);
            mbuf_copy(dst, pos, n);
            pos += n;
            length -= n;
            remain -= n;
        }
    }
    
    return msg;
}

static char *
memcache_type_string(msg_type_t type)
{
    switch (type) {
    case MSG_REQ_MC_DELETE:
        return "delete";
    default:
        return NULL;
    }
}

static struct msg *
memcache_build_notify(struct msg *req)
{
    struct msg *msg;
    struct conn *c_conn;
    struct server_pool *pool;
    struct mbuf *mbuf;
    char *cmd;
    int n;

    c_conn = req->owner;
    pool = c_conn->owner;
    cmd = memcache_type_string(req->type);

    ASSERT(c_conn != NULL && pool != NULL && cmd != NULL);

    msg = msg_get(NULL, true, c_conn->redis);
    if (msg == NULL) {
        return NULL;
    }

    mbuf = mbuf_get();
    if (mbuf == NULL) {
        msg_put(msg);
        return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    n = nc_scnprintf(mbuf->last, mbuf_size(mbuf), 
                     "*3\r\n"
                     "$5\r\n"
                     "LPUSH\r\n"
                     "$%d\r\n"
                     "%.*s\r\n" /* pid */
                     "$%d\r\n"
                     "%s %.*s\r\n", /* "cmd req_key" */
                     pool->name.len,
                     pool->name.len, pool->name.data,
                     strlen(cmd) + 1 + (req->key_end - req->key_start),
                     cmd, 
                     req->key_end - req->key_start, req->key_start);
    log_debug(LOG_VERB, "notify: %.*s", n, mbuf->last);
    mbuf->last += n;
    ASSERT(mbuf->last <= mbuf->end);
    
    msg->swallow = 1;

    return msg;
}


rstatus_t
memcache_pre_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    rstatus_t status;
    struct server_pool *pool, *mq;
    struct msg *n_msg;
    struct conn *conn;

    ASSERT(c_conn->client && !c_conn->proxy);

    pool = c_conn->owner;
    mq = pool->message_queue;
    if (mq == NULL) {
        return NC_OK;
    }
    
    /* Only delete requests need to be notified */
    if (!memcache_delete(msg)) {
        return NC_OK;
    }

    conn = server_pool_conn(ctx, mq, msg->key_start,
                            (uint32_t)(msg->key_end - msg->key_start));
    if (conn == NULL) {
        log_error("failed to fetch connection for \"%.*s\"", 
                  pool->name.len, pool->name.data);
        return NC_ERROR;
    }
    
    n_msg = memcache_build_notify(msg);
    if (n_msg == NULL) {
        log_error("failed to build notify message for \"%.*s\"", 
                  pool->name.len, pool->name.data);
        return NC_ERROR;
    }

    status = req_enqueue(ctx, conn, n_msg);
    if (status != NC_OK) {
        req_put(n_msg);
        return status;
    }

    return NC_OK;
}

struct conn *
memcache_routing(struct context *ctx, struct server_pool *pool, 
                 struct msg *msg, struct string *key)
{
    struct conn *s_conn, *f_conn;
    struct server_pool *gutter, *peer;
    
    s_conn = server_pool_conn(ctx, pool, key->data, key->len);

    /* Automatic failover logic */
    if (s_conn == NULL) {       
        gutter = pool->gutter;
        /* Fallback to the gutter pool */
        if (gutter != NULL) {
            f_conn = server_pool_conn(ctx, gutter, key->data, key->len);
            if (f_conn != NULL) {
                log_debug(LOG_VERB, "fallback to gutter connection");
                return f_conn;
            }
        } 

        return NULL;
    } 

    /* Automatic warmup logic */
    if (memcache_cold(s_conn)) { 
        peer = pool->peer;
        /* Fallback to the peer pool if possible */
        if (peer != NULL) {
            f_conn = server_pool_conn(ctx, peer, key->data, key->len);
            if (f_conn != NULL && !memcache_cold(f_conn)) {
                /* Record the original target */
                msg->origin = s_conn;
                log_debug(LOG_VERB, "fallback to peer connection");
                return f_conn;
            }
        }
    }

    return s_conn;
}

/* Always return NC_OK */
rstatus_t
memcache_post_routing(struct context *ctx, struct conn *s_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *clone;

    if (msg->origin == NULL) {
        return NC_OK;
    }

    clone = msg_clone(msg);
    if (clone == NULL) {
        return NC_OK;
    }

    clone->owner = NULL;        /* Special purpose request */
    clone->swallow = 1;         /* Discard the response */
    
    status = req_enqueue(ctx, s_conn, clone);
    if (status != NC_OK) {
        req_put(clone);
    }

    return NC_OK;
}

rstatus_t
memcache_post_rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;
    struct conn *c_conn;
    
    pmsg = msg->peer;           /* request */
    c_conn = pmsg->owner;

    /* Handle probe response */
    if (c_conn == NULL) {
        memcache_handle_probe(pmsg, msg);
        req_put(pmsg);
        return NC_OK;
    }

    /* If the request and response belong to different pools, either
     * we have a connection to be warmup up, or the response came from a
     * gutter
     */
    if (c_conn->owner == s_conn->owner || pmsg->origin == NULL) {
        return NC_OK;
    }

    if (!memcache_need_warmup(pmsg, msg)) {
        return NC_OK;
    }

    msg = memcache_build_warmup(pmsg, msg);
    if (msg == NULL) {
        return NC_OK;
    }
    
    msg->noreply = 1;
    msg->swallow = 1;

    status = req_enqueue(ctx, pmsg->origin, msg);
    if (status != NC_OK) {
        req_put(msg);
    }
    
    return NC_OK;
}
