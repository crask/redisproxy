#include <stdio.h>
#include <stdlib.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>

#define DEFAULT_PARTITION_SIZE 2

static rstatus_t
alloc_layer2_continum(struct array *pc, uint32_t npartition, uint32_t ntags)
{
    rstatus_t status;
    uint32_t pidx;

    status = array_init(pc, npartition, sizeof(struct array));
    if (status != NC_OK) {
        return status;
    }

    for (pidx = 0; pidx < npartition; pidx++) {
        uint32_t tag_idx;
        struct array *alive_servers, *tagged_continuum;

        tagged_continuum = array_push(pc);
        status = array_init(tagged_continuum, ntags, sizeof(struct array));
        if (status != NC_OK) {
            return status;
        }
            
        for (tag_idx = 0; tag_idx < ntags; tag_idx++) {
            alive_servers = array_push(tagged_continuum);
            status = array_init(alive_servers, DEFAULT_PARTITION_SIZE, sizeof(struct continuum));
            if (status != NC_OK) {
                return status;
            }
        }
    }

    return NC_OK;
}

static rstatus_t
reconstruct_tagged_continuum(struct array *tagged_continuum, struct server_pool *pool,
                             struct array *partition, int flag, int64_t now) 
{
    uint32_t i, nserver, ntags;
    struct array *alives;
    struct server *server;

    ntags = array_n(tagged_continuum);
    for (i = 0; i < ntags; i++) {
        array_rewind(array_get(tagged_continuum, i));
    }

    nserver = array_n(partition);   /* totol servers */
    for (i = 0; i < nserver; i++) {
        struct continuum *continuum, *alive_continuum;

        continuum = array_get(partition, i);
        server = array_get(&pool->server, continuum->index);
            
        if (pool->auto_eject_hosts && server->next_retry > now) {
            continue;
        }

        /* push into the alive partition */
        if (server->flags & flag) {
            alives = array_get(tagged_continuum, server->tag_idx);
            alive_continuum = array_push(alives);
            alive_continuum->index = continuum->index;
            alive_continuum->value = continuum->value;
        }
    }

    return NC_OK;
}

rstatus_t
range_update(struct server_pool *pool)
{
    rstatus_t status;
    uint32_t nserver, nlive_server, npartition, ntags;
    int64_t now;
    uint32_t server_index, partition_index, continuum_index;
    struct server *server;
    struct continuum *continuum, *continuums;
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

    /* Allocate the partition continuum only for the first time */
    if (npartition > pool->npartition_continuum) {
        /* Allocate the layer 1 continuum */
        continuums = nc_realloc(pool->continuum, sizeof(*continuum) * npartition);
        if (continuums == NULL) {
            return NC_ENOMEM;
        }
        
        pool->continuum = continuums;
        pool->ncontinuum = npartition;

        /* Initialize the layer 1 continuum */
        continuum_index = 0;
        for (partition_index = 0; partition_index < npartition; partition_index++) {
            partition = array_get(&pool->partition, partition_index);
            ASSERT(array_n(partition) > 0);
            
            continuum = array_get(partition, 0);

            pool->continuum[continuum_index].index = partition_index;
            pool->continuum[continuum_index].value = continuum->value;
            
            continuum_index++;
        }

        pool->npartition_continuum = npartition;

        /* Allocate the layer 2 partition continuum */
        ntags = array_n(&pool->tags);
        status = alloc_layer2_continum(&pool->r_partition_continuum, npartition, ntags);
        if (status != NC_OK) {
            return status;
        }

        status = alloc_layer2_continum(&pool->w_partition_continuum, npartition, ntags);
        if (status != NC_OK) {
            return status;
        }
    }
    
    /* Construct the layer 2 partition continuum */
    for (partition_index = 0; partition_index < npartition; partition_index++) {
        struct array *tagged_continuum;

        partition = array_get(&pool->partition, partition_index); /* live and dead */

        tagged_continuum = array_get(&pool->r_partition_continuum, partition_index);
        reconstruct_tagged_continuum(tagged_continuum, pool, partition, NC_SERVER_READABLE, now);

        tagged_continuum = array_get(&pool->w_partition_continuum, partition_index);
        reconstruct_tagged_continuum(tagged_continuum, pool, partition, NC_SERVER_WRITABLE, now);
    }
    
    log_debug(LOG_VERB, "updated pool %"PRIu32" '%.*s' with %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data, nserver);
    return NC_OK;
}

int
range_dispatch(struct server_pool *pool, struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *left, *right, *middle, *c;
    struct array *p, *tagged_continuum;
    uint32_t nserver;

    hash &= DIST_RANGE_MAX - 1;         /* only keep the low 16 bits */
    left = continuum - 1;
    right = continuum + ncontinuum - 1;

    ASSERT(ncontinuum > 0);

    /* Search in the layer 1 continuum */
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

    /* Search in the layer 2 continuum */
    tagged_continuum = array_get(pool->partition_continuum, right->index);
    p = array_get(tagged_continuum, pool->tag_idx);
    nserver = array_n(p);

    /* if no alive server in the continuum with local_tag, failover to other tags */
    if (nserver == 0) {
        int i, tag_idx;

        for (i = 0; i < MAX_FAILOVER_TAGS; i++) {
            tag_idx = pool->fo_tag_idx[i];
            if (tag_idx < 0) continue;
            p = array_get(tagged_continuum, tag_idx);
            nserver = array_n(p);
            if (nserver > 0) break;
        }

        if (nserver == 0) {
            errno = NC_ESERVICEUNAVAILABLE;
            log_debug(LOG_VERB, "no alive server in partition %d", right->index);
            return -1;
        }
    }
    
    ASSERT(array_n(p) > 0);

    /* Random load balancing */
    log_debug(LOG_VVVERB, "pick from %d servers %d", nserver);
    if (nserver > 1) {
        c = array_get(p, random() % array_n(p));
    } else {
        c = array_get(p, 0);
    }

    log_debug(LOG_VVERB, "dispatch hash %"PRIu32" to index %"PRIu32,
              hash, c->index);
    return (int)c->index;
}
