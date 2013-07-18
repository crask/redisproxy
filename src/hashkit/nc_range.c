#include <stdio.h>
#include <stdlib.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>

rstatus_t
range_update(struct server_pool *pool)
{
    uint32_t nserver;
    int64_t now;
    uint32_t server_index, continuum_index;
    struct server *server;
    struct continuum *continuum;

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    nserver = array_n(&pool->server);
    pool->next_rebuild = 0LL;
    
    for (server_index = 0; server_index < nserver; server_index++) {
        server = array_get(&pool->server, server_index);

        if (pool->next_rebuild == 0LL ||
            server->next_retry < pool->next_rebuild) {
            pool->next_rebuild = server->next_retry;
        }
    }

    /* Allocate the continuum for the first time */
    if (nserver > pool->nserver_continuum) {
        continuum = nc_realloc(pool->continuum, sizeof(*continuum) * nserver);
        if (continuum == NULL) {
            return NC_ENOMEM;
        }

        pool->continuum = continuum;
        pool->nserver_continuum = nserver;
    }

    continuum_index = 0;
    for (server_index = 0; server_index < nserver; server_index++) {
        server = array_get(&pool->server, server_index);

        pool->continuum[continuum_index].index = server_index;
        pool->continuum[continuum_index++].value = server->range_end;
    }
    pool->ncontinuum = nserver;
    
    log_debug(LOG_VERB, "updated pool %"PRIu32" '%.*s' with %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data, nserver);
    return NC_OK;
}

uint32_t
range_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *left, *right, *middle;

    hash &= DIST_RANGE_MAX - 1;
    left = continuum;
    right = continuum + ncontinuum - 1;

    while (left < right) {
        middle = left + (right - left) / 2;
        if (middle->value <= hash) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    return left->index;
}
