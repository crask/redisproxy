
#line 1 "nc_confx.rl"
#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <proto/nc_proto.h>
#include <nc_confx.h>


#line 11 "nc_confx.c"
static const char _server_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	3, 1, 4, 1, 5, 1, 6, 1, 
	7, 1, 10, 1, 11, 1, 12, 1, 
	13, 2, 8, 7, 2, 12, 13, 3, 
	9, 12, 13
};

static const char _server_key_offsets[] = {
	0, 0, 8, 13, 23, 26, 28, 31, 
	33, 36, 38, 41, 43, 46, 48, 51, 
	61, 66, 71, 74, 84, 89, 100
};

static const char _server_trans_keys[] = {
	47, 95, 48, 57, 65, 90, 97, 122, 
	95, 65, 90, 97, 122, 45, 47, 58, 
	95, 48, 57, 65, 90, 97, 122, 58, 
	48, 57, 48, 57, 45, 48, 57, 48, 
	57, 46, 48, 57, 48, 57, 46, 48, 
	57, 48, 57, 46, 48, 57, 48, 57, 
	58, 48, 57, 45, 46, 58, 95, 48, 
	57, 65, 90, 97, 122, 95, 65, 90, 
	97, 122, 32, 9, 13, 48, 57, 32, 
	9, 13, 32, 95, 9, 13, 48, 57, 
	65, 90, 97, 122, 32, 9, 13, 48, 
	57, 32, 45, 95, 9, 13, 48, 57, 
	65, 90, 97, 122, 32, 9, 13, 48, 
	57, 0
};

static const char _server_single_lengths[] = {
	0, 2, 1, 4, 1, 0, 1, 0, 
	1, 0, 1, 0, 1, 0, 1, 4, 
	1, 1, 1, 2, 1, 3, 1
};

static const char _server_range_lengths[] = {
	0, 3, 2, 3, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 3, 
	2, 2, 1, 4, 2, 4, 2
};

static const char _server_index_offsets[] = {
	0, 0, 6, 10, 18, 21, 23, 26, 
	28, 31, 33, 36, 38, 41, 43, 46, 
	54, 58, 62, 65, 72, 76, 84
};

static const char _server_indicies[] = {
	1, 3, 2, 3, 3, 0, 4, 4, 
	4, 0, 4, 6, 7, 4, 4, 4, 
	4, 5, 9, 8, 5, 11, 10, 12, 
	13, 5, 15, 14, 16, 17, 0, 18, 
	0, 19, 18, 0, 20, 0, 21, 20, 
	0, 22, 0, 7, 22, 5, 23, 24, 
	7, 23, 23, 23, 23, 5, 23, 23, 
	23, 0, 27, 26, 11, 25, 28, 28, 
	25, 30, 31, 28, 13, 31, 31, 29, 
	28, 28, 15, 25, 32, 33, 33, 28, 
	33, 33, 33, 25, 32, 28, 13, 34, 
	0
};

static const char _server_trans_targs[] = {
	0, 2, 8, 15, 3, 0, 2, 4, 
	4, 5, 0, 17, 7, 6, 0, 20, 
	9, 8, 10, 11, 12, 13, 14, 15, 
	16, 0, 18, 19, 18, 0, 19, 21, 
	22, 21, 0
};

static const char _server_trans_actions[] = {
	5, 1, 1, 1, 0, 0, 0, 3, 
	7, 0, 13, 9, 0, 17, 21, 19, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 23, 11, 11, 0, 31, 0, 25, 
	0, 15, 28
};

static const char _server_eof_actions[] = {
	0, 5, 5, 0, 0, 13, 0, 21, 
	5, 5, 5, 5, 5, 5, 0, 0, 
	5, 11, 0, 0, 0, 0, 0
};

static const int server_start = 1;
static const int server_first_final = 17;
static const int server_error = 0;

static const int server_en_main = 1;


#line 10 "nc_confx.rl"


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

    
#line 171 "nc_confx.c"
	{
	cs = server_start;
	}

#line 176 "nc_confx.c"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
	if ( cs == 0 )
		goto _out;
_resume:
	_keys = _server_trans_keys + _server_key_offsets[cs];
	_trans = _server_index_offsets[cs];

	_klen = _server_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _server_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _server_indicies[_trans];
	cs = _server_trans_targs[_trans];

	if ( _server_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _server_actions + _server_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 0:
#line 75 "nc_confx.rl"
	{
            addr = p;
            pname = p;
        }
	break;
	case 1:
#line 80 "nc_confx.rl"
	{
            addrlen = (uint32_t)(p - addr);
        }
	break;
	case 2:
#line 84 "nc_confx.rl"
	{
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }
	break;
	case 3:
#line 89 "nc_confx.rl"
	{
            port = port * 10 + ((*p) - '0'); 
            portlen++;
        }
	break;
	case 4:
#line 99 "nc_confx.rl"
	{
            weight = weight * 10 + ((*p) - '0');
        }
	break;
	case 5:
#line 103 "nc_confx.rl"
	{
            pnamelen = p - pname;
        }
	break;
	case 6:
#line 107 "nc_confx.rl"
	{
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }
	break;
	case 7:
#line 112 "nc_confx.rl"
	{
            namelen++;
        }
	break;
	case 8:
#line 116 "nc_confx.rl"
	{
            name = p;
        }
	break;
	case 9:
#line 120 "nc_confx.rl"
	{
            log_error("conf: invalid name");
            status = NC_ERROR;
        }
	break;
	case 10:
#line 125 "nc_confx.rl"
	{
            rstart = rstart * 10 + ((*p) - '0');
        }
	break;
	case 11:
#line 129 "nc_confx.rl"
	{
            rend = rend * 10 + ((*p) - '0');
        }
	break;
	case 12:
#line 133 "nc_confx.rl"
	{
            log_error("conf: invalid range");
            status = NC_ERROR;
        }
	break;
	case 13:
#line 138 "nc_confx.rl"
	{
            log_error("conf: invalid server conf");
            status = NC_ERROR;
        }
	break;
#line 341 "nc_confx.c"
		}
	}

_again:
	if ( cs == 0 )
		goto _out;
	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	const char *__acts = _server_actions + _server_eof_actions[cs];
	unsigned int __nacts = (unsigned int) *__acts++;
	while ( __nacts-- > 0 ) {
		switch ( *__acts++ ) {
	case 2:
#line 84 "nc_confx.rl"
	{
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }
	break;
	case 5:
#line 103 "nc_confx.rl"
	{
            pnamelen = p - pname;
        }
	break;
	case 6:
#line 107 "nc_confx.rl"
	{
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }
	break;
	case 12:
#line 133 "nc_confx.rl"
	{
            log_error("conf: invalid range");
            status = NC_ERROR;
        }
	break;
#line 384 "nc_confx.c"
		}
	}
	}

	_out: {}
	}

#line 173 "nc_confx.rl"


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
