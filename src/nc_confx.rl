#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <proto/nc_proto.h>
#include <nc_confx.h>

%%{
    machine server;
    write data;
}%%

void
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    cs->port = 0;
    cs->weight = 0;
    cs->start = 0;
    cs->end = 0;

    memset(&cs->info, 0, sizeof(cs->info));

    cs->valid = 0;

    log_debug(LOG_VVERB, "init conf server %p", cs);
}

void
conf_server_deinit(struct conf_server *cs)
{
    string_deinit(&cs->pname);
    string_deinit(&cs->name);
    cs->valid = 0;
    log_debug(LOG_VVERB, "deinit conf server %p", cs);
}

char *
conf_add_server(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *addr, *pname, *name;
    uint32_t addrlen, pnamelen, namelen, portlen;
    int port, weight, rstart, rend;
    struct array *a;
    struct string address;
    struct string *value;
    struct conf_server *field;
    
    int cs;
    uint8_t *p, *pe, *eof;
    
    addr = NULL;
    addrlen = 0;
    pname = NULL;
    pnamelen = 0;
    name = NULL;
    namelen = 0;
    port = 0;
    portlen = 0;
    weight = 0;
    rstart = 0;
    rend = 0;

    status = NC_OK;

    value = array_top(&cf->arg);    

    /* ragel interface variables */
    p = value->data;
    pe = value->data + value->len;
    eof = pe;

    %%{
        action addr_start {
            addr = fpc;
            pname = fpc;
        }

        action addr_end {
            addrlen = (uint32_t)(fpc - addr);
        }

        action addr_error {
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }

        action port {
            port = port * 10 + (fc - '0'); 
            portlen++;
        }
        
        action port_error {
            log_error("conf: invalid port");
            status = NC_ERROR;
        }
        
        action weight {
            weight = weight * 10 + (fc - '0');
        }
        
        action weight_end {
            pnamelen = fpc - pname;
        }
               
        action weight_error {
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }

        action name {
            namelen++;
        }
        
        action name_enter {
            name = fpc;
        }

        action name_error {
            log_error("conf: invalid name");
            status = NC_ERROR;
        }

        action range_start {
            rstart = rstart * 10 + (fc - '0');
        }
        
        action range_end {
            rend = rend * 10 + (fc - '0');
        }

        action range_error {
            log_error("conf: invalid range");
            status = NC_ERROR;
        }

        action error {
            log_error("conf: invalid server conf");
            status = NC_ERROR;
        }

        identifier = [a-zA-Z_][a-zA-Z_0-9\-]*;

        ip = (digit+ ('.' digit+){3}) >addr_start %addr_end @!addr_error;

        host = (identifier ('.' identifier)*) >addr_start %addr_end @!addr_error;

        path = ('/' identifier)+ >addr_start %addr_end @!addr_error;

        port = (digit @port)* @!port_error;

        weight = (digit @weight)+ %weight_end @!weight_error;

        name = identifier @name >name_enter @!name_error;
                
        rstart = (digit @range_start)+ @!range_error;

        rend = (digit @range_end)+ @!range_error;

        range = rstart '-' rend;

        basic = (ip | host | path) ':' port ':' weight;

        optional_name = (' '+ name)?;

        optional_range = (' '+ range)?;
            
        main := basic optional_name optional_range space* $!error;

        write init;
        write exec;
    }%%

    if (status != NC_OK) {
        return CONF_ERROR;
    }

    p = conf;
    a = (struct array *)(p + cmd->offset);

    field = array_push(a);
    if (field == NULL) {
        return CONF_ERROR;
    }

    conf_server_init(field);
    
    field->start = rstart;
    field->end = rend;
    field->weight = weight;
    field->port = port;
    
    status = string_copy(&field->pname, pname, pnamelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    string_init(&address);

    status = string_copy(&address, addr, addrlen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&address, field->port, &field->info);
    if (status != NC_OK) {
        log_error("conf: failed to resolve %.*s:%d", address.len, address.data, field->port);
        string_deinit(&address);
        return CONF_ERROR;
    }
    
    string_deinit(&address);
    field->valid = 1;
    
    return CONF_OK;
}
