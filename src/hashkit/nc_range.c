#include <stdio.h>
#include <stdlib.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>

rstatus_t
range_update(struct server_pool *pool)
{
    uint32_t nserver, nlive_server, npartition;
    int64_t now;
    uint32_t server_index, partition_index, continuum_index;
    struct server *server;
    struct continuum *continuum, *c;
    struct array *partition;

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    nserver = array_n(&pool->server);
    npartition = array_n(&pool->partition);
    nlive_server = 0;
    pool->next_rebuild = 0LL;

    ASSERT(nserver > 0);

    if (pool->auto_eject_hosts) {
        for (server_index = 0; server_index < nserver; server_index++) {
            server = array_get(&pool->server, server_index);

            if (server->next_retry <= now) {
                server->next_retry = 0LL;
                nlive_server++;
            } else if (pool->next_rebuild == 0LL ||
                       server->next_retry < pool->next_rebuild) {
                pool->next_rebuild = server->next_retry;
            }
        }
    } else {
        nlive_server = nserver;
    }
    
    pool->nlive_server = nlive_server;
    
    if (nlive_server == 0) {
        ASSERT(pool->continuum != NULL);
        ASSERT(pool->ncontinuum != 0);

        log_debug(LOG_DEBUG, "no live servers for pool %"PRIu32" '%.*s'",
                  pool->idx, pool->name.len, pool->name.data);

        return NC_OK;
    }
    
    /* Allocate the continuum for the first time */
    if (npartition > pool->nserver_continuum) {
        continuum = nc_realloc(pool->continuum, sizeof(*continuum) * npartition);
        if (continuum == NULL) {
            return NC_ENOMEM;
        }

        pool->continuum = continuum;
        pool->nserver_continuum = npartition;
        pool->ncontinuum = npartition;
        
        continuum_index = 0;
        for (partition_index = 0; partition_index < npartition; partition_index++) {
            partition = array_get(&pool->partition, partition_index);

            ASSERT(array_n(partition) > 0);
            c = array_get(partition, 0);
            
            pool->continuum[continuum_index].index = partition_index;
            pool->continuum[continuum_index++].value = c->value;
        }
    }
    
    log_debug(LOG_VERB, "updated pool %"PRIu32" '%.*s' with %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data, nserver);
    return NC_OK;
}


uint32_t
range_dispatch(struct server_pool *pool, struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *left, *right, *middle, *c;
    struct array *p;

    hash &= DIST_RANGE_MAX - 1;         /* only keep the low 16 bits */
    left = continuum - 1;
    right = continuum + ncontinuum - 1;

    ASSERT(ncontinuum > 0);
    
    /* hash in [left, right) */
    while (right - left > 1) {
        middle = left + (right - left) / 2;
        /* left < middle < right  and middle >= continuum */
        if (middle->value <= hash) {
            /* hash in [middle, right) */
            left = middle;
            /* hash in [left, right) */
        } else {
            /* hash in [left, middle) */
            right = middle;
            /* hash in [left, right) */
        }
    }    
    /* hash in [left, right) and right - left = 1 */
    ASSERT(right->index < ncontinuum);

    p = array_get(&pool->partition, right->index);

    ASSERT(array_n(p) > 0);

    c = array_get(p, random() % array_n(p));

    log_debug(LOG_VVERB, "dispatch hash %"PRIu32" to index %"PRIu32,
              hash, c->index);
    return c->index;
}
