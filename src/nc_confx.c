
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
	13, 1, 14, 1, 15, 1, 16, 2, 
	8, 7, 2, 12, 13, 3, 9, 12, 
	13
};

static const char _server_key_offsets[] = {
	0, 0, 8, 13, 23, 26, 28, 31, 
	33, 35, 37, 38, 41, 43, 46, 48, 
	51, 53, 56, 66, 71, 77, 80, 90, 
	95, 106, 111
};

static const char _server_trans_keys[] = {
	47, 95, 48, 57, 65, 90, 97, 122, 
	95, 65, 90, 97, 122, 45, 47, 58, 
	95, 48, 57, 65, 90, 97, 122, 58, 
	48, 57, 48, 57, 45, 48, 57, 48, 
	57, 114, 119, 111, 119, 111, 46, 48, 
	57, 48, 57, 46, 48, 57, 48, 57, 
	46, 48, 57, 48, 57, 58, 48, 57, 
	45, 46, 58, 95, 48, 57, 65, 90, 
	97, 122, 95, 65, 90, 97, 122, 32, 
	58, 9, 13, 48, 57, 32, 9, 13, 
	32, 95, 9, 13, 48, 57, 65, 90, 
	97, 122, 32, 9, 13, 48, 57, 32, 
	45, 95, 9, 13, 48, 57, 65, 90, 
	97, 122, 32, 9, 13, 48, 57, 32, 
	9, 13, 0
};

static const char _server_single_lengths[] = {
	0, 2, 1, 4, 1, 0, 1, 0, 
	2, 2, 1, 1, 0, 1, 0, 1, 
	0, 1, 4, 1, 2, 1, 2, 1, 
	3, 1, 1
};

static const char _server_range_lengths[] = {
	0, 3, 2, 3, 1, 1, 1, 1, 
	0, 0, 0, 1, 1, 1, 1, 1, 
	1, 1, 3, 2, 2, 1, 4, 2, 
	4, 2, 1
};

static const char _server_index_offsets[] = {
	0, 0, 6, 10, 18, 21, 23, 26, 
	28, 31, 34, 36, 39, 41, 44, 46, 
	49, 51, 54, 62, 66, 71, 74, 81, 
	85, 93, 97
};

static const char _server_indicies[] = {
	1, 3, 2, 3, 3, 0, 4, 4, 
	4, 0, 4, 6, 7, 4, 4, 4, 
	4, 5, 9, 8, 5, 11, 10, 12, 
	13, 5, 15, 14, 16, 17, 5, 18, 
	19, 5, 20, 5, 21, 22, 0, 23, 
	0, 24, 23, 0, 25, 0, 26, 25, 
	0, 27, 0, 7, 27, 5, 28, 29, 
	7, 28, 28, 28, 28, 5, 28, 28, 
	28, 0, 32, 33, 31, 11, 30, 34, 
	34, 30, 36, 37, 34, 13, 37, 37, 
	35, 34, 34, 15, 30, 38, 39, 39, 
	34, 39, 39, 39, 30, 38, 34, 13, 
	40, 36, 34, 30, 0
};

static const char _server_trans_targs[] = {
	0, 2, 11, 18, 3, 0, 2, 4, 
	4, 5, 0, 20, 7, 6, 0, 23, 
	9, 10, 26, 26, 26, 12, 11, 13, 
	14, 15, 16, 17, 18, 19, 0, 21, 
	22, 8, 21, 0, 22, 24, 25, 24, 
	0
};

static const char _server_trans_actions[] = {
	5, 1, 1, 1, 0, 0, 0, 3, 
	7, 0, 13, 9, 0, 17, 21, 19, 
	0, 0, 27, 25, 29, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 23, 11, 
	11, 11, 0, 37, 0, 31, 0, 15, 
	34
};

static const char _server_eof_actions[] = {
	0, 5, 5, 0, 0, 13, 0, 21, 
	0, 0, 0, 5, 5, 5, 5, 5, 
	5, 0, 0, 5, 11, 0, 0, 0, 
	0, 0, 0
};

static const int server_start = 1;
static const int server_first_final = 20;
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
    int port, weight, rstart, rend, flags;
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
    flags = NC_SERVER_READABLE | NC_SERVER_WRITABLE; /* default rw */
    rstart = 0;
    rend = 0;

    status = NC_OK;

    value = array_top(&cf->arg);    

    /* ragel interface variables */
    p = value->data;
    pe = value->data + value->len;
    eof = pe;

    
#line 182 "nc_confx.c"
	{
	cs = server_start;
	}

#line 187 "nc_confx.c"
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
#line 76 "nc_confx.rl"
	{
            addr = p;
            pname = p;
        }
	break;
	case 1:
#line 81 "nc_confx.rl"
	{
            addrlen = (uint32_t)(p - addr);
        }
	break;
	case 2:
#line 85 "nc_confx.rl"
	{
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }
	break;
	case 3:
#line 90 "nc_confx.rl"
	{
            port = port * 10 + ((*p) - '0'); 
            portlen++;
        }
	break;
	case 4:
#line 100 "nc_confx.rl"
	{
            weight = weight * 10 + ((*p) - '0');
        }
	break;
	case 5:
#line 104 "nc_confx.rl"
	{
            pnamelen = p - pname;
        }
	break;
	case 6:
#line 108 "nc_confx.rl"
	{
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }
	break;
	case 7:
#line 113 "nc_confx.rl"
	{
            namelen++;
        }
	break;
	case 8:
#line 117 "nc_confx.rl"
	{
            name = p;
        }
	break;
	case 9:
#line 121 "nc_confx.rl"
	{
            log_error("conf: invalid name");
            status = NC_ERROR;
        }
	break;
	case 10:
#line 126 "nc_confx.rl"
	{
            rstart = rstart * 10 + ((*p) - '0');
        }
	break;
	case 11:
#line 130 "nc_confx.rl"
	{
            rend = rend * 10 + ((*p) - '0');
        }
	break;
	case 12:
#line 134 "nc_confx.rl"
	{
            log_error("conf: invalid range");
            status = NC_ERROR;
        }
	break;
	case 13:
#line 139 "nc_confx.rl"
	{
            log_error("conf: invalid server conf");
            status = NC_ERROR;
        }
	break;
	case 14:
#line 144 "nc_confx.rl"
	{ flags = NC_SERVER_READABLE | NC_SERVER_WRITABLE; }
	break;
	case 15:
#line 145 "nc_confx.rl"
	{ flags = NC_SERVER_READABLE; }
	break;
	case 16:
#line 146 "nc_confx.rl"
	{ flags = NC_SERVER_WRITABLE; }
	break;
#line 364 "nc_confx.c"
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
#line 85 "nc_confx.rl"
	{
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }
	break;
	case 5:
#line 104 "nc_confx.rl"
	{
            pnamelen = p - pname;
        }
	break;
	case 6:
#line 108 "nc_confx.rl"
	{
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }
	break;
	case 12:
#line 134 "nc_confx.rl"
	{
            log_error("conf: invalid range");
            status = NC_ERROR;
        }
	break;
#line 407 "nc_confx.c"
		}
	}
	}

	_out: {}
	}

#line 180 "nc_confx.rl"


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
    field->flags = flags;
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
