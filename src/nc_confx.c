
#line 1 "src/nc_confx.rl"
#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <proto/nc_proto.h>
#include <nc_confx.h>


#line 11 "src/nc_confx.c"
static const char _server_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	3, 1, 4, 1, 5, 1, 6, 1, 
	7, 1, 8, 1, 11, 1, 12, 1, 
	13, 1, 14, 1, 15, 1, 16, 1, 
	17, 1, 18, 1, 19, 1, 20, 1, 
	21, 2, 9, 8, 2, 13, 14, 3, 
	10, 13, 14
};

static const char _server_key_offsets[] = {
	0, 0, 8, 13, 23, 27, 30, 32, 
	33, 39, 42, 44, 46, 48, 51, 54, 
	56, 59, 61, 64, 66, 69, 79, 84, 
	95, 98, 108, 113, 124
};

static const char _server_trans_keys[] = {
	47, 95, 48, 57, 65, 90, 97, 122, 
	95, 65, 90, 97, 122, 45, 47, 58, 
	95, 48, 57, 65, 90, 97, 122, 32, 
	58, 48, 57, 32, 45, 114, 45, 119, 
	32, 32, 95, 65, 90, 97, 122, 45, 
	48, 57, 48, 57, 45, 119, 48, 57, 
	32, 48, 57, 46, 48, 57, 48, 57, 
	46, 48, 57, 48, 57, 46, 48, 57, 
	48, 57, 58, 48, 57, 45, 46, 58, 
	95, 48, 57, 65, 90, 97, 122, 95, 
	65, 90, 97, 122, 32, 45, 95, 9, 
	13, 48, 57, 65, 90, 97, 122, 32, 
	9, 13, 32, 95, 9, 13, 48, 57, 
	65, 90, 97, 122, 32, 9, 13, 48, 
	57, 32, 45, 95, 9, 13, 48, 57, 
	65, 90, 97, 122, 32, 9, 13, 48, 
	57, 0
};

static const char _server_single_lengths[] = {
	0, 2, 1, 4, 2, 3, 2, 1, 
	2, 1, 0, 2, 0, 1, 1, 0, 
	1, 0, 1, 0, 1, 4, 1, 3, 
	1, 2, 1, 3, 1
};

static const char _server_range_lengths[] = {
	0, 3, 2, 3, 1, 0, 0, 0, 
	2, 1, 1, 0, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 3, 2, 4, 
	1, 4, 2, 4, 2
};

static const unsigned char _server_index_offsets[] = {
	0, 0, 6, 10, 18, 22, 26, 29, 
	31, 36, 39, 41, 44, 46, 49, 52, 
	54, 57, 59, 62, 64, 67, 75, 79, 
	87, 90, 97, 101, 109
};

static const char _server_indicies[] = {
	1, 3, 2, 3, 3, 0, 4, 4, 
	4, 0, 4, 6, 7, 4, 4, 4, 
	4, 5, 8, 10, 9, 5, 11, 12, 
	13, 5, 14, 15, 5, 16, 5, 16, 
	18, 18, 18, 17, 19, 20, 5, 22, 
	21, 23, 24, 5, 26, 25, 27, 26, 
	5, 28, 29, 0, 30, 0, 31, 30, 
	0, 32, 0, 33, 32, 0, 34, 0, 
	7, 34, 5, 35, 36, 7, 35, 35, 
	35, 35, 5, 35, 35, 35, 0, 39, 
	40, 40, 38, 40, 40, 40, 37, 41, 
	41, 37, 43, 44, 41, 20, 44, 44, 
	42, 41, 41, 22, 37, 45, 46, 46, 
	41, 46, 46, 46, 37, 45, 41, 20, 
	47, 0
};

static const char _server_trans_targs[] = {
	0, 2, 14, 21, 3, 0, 2, 4, 
	5, 4, 12, 5, 6, 11, 7, 7, 
	8, 0, 23, 10, 9, 0, 26, 7, 
	7, 0, 13, 5, 15, 14, 16, 17, 
	18, 19, 20, 21, 22, 0, 24, 25, 
	23, 24, 0, 25, 27, 28, 27, 0
};

static const char _server_trans_actions[] = {
	5, 1, 1, 1, 0, 0, 0, 3, 
	9, 7, 9, 0, 0, 0, 39, 37, 
	0, 31, 27, 0, 19, 23, 21, 35, 
	33, 15, 11, 13, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 25, 29, 29, 
	0, 0, 47, 0, 41, 0, 17, 44
};

static const char _server_eof_actions[] = {
	0, 5, 5, 0, 0, 0, 0, 0, 
	31, 0, 23, 0, 15, 0, 5, 5, 
	5, 5, 5, 5, 0, 0, 5, 29, 
	0, 0, 0, 0, 0
};

static const int server_start = 1;
static const int server_first_final = 23;
static const int server_error = 0;

static const int server_en_main = 1;


#line 10 "src/nc_confx.rl"


void
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    string_init(&cs->tag);
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
    uint8_t *addr, *pname, *name, *tag;
    uint32_t addrlen, pnamelen, namelen, portlen, taglen;
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
    tag = NULL;
    taglen = 0;

    status = NC_OK;

    value = array_top(&cf->arg);    

    /* ragel interface variables */
    p = value->data;
    pe = value->data + value->len;
    eof = pe;

    
#line 190 "src/nc_confx.c"
	{
	cs = server_start;
	}

#line 195 "src/nc_confx.c"
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
#line 79 "src/nc_confx.rl"
	{
            addr = p;
            pname = p;
        }
	break;
	case 1:
#line 84 "src/nc_confx.rl"
	{
            addrlen = (uint32_t)(p - addr);
        }
	break;
	case 2:
#line 88 "src/nc_confx.rl"
	{
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }
	break;
	case 3:
#line 93 "src/nc_confx.rl"
	{
            port = port * 10 + ((*p) - '0'); 
            portlen++;
        }
	break;
	case 4:
#line 98 "src/nc_confx.rl"
	{
            pnamelen = p - pname;
        }
	break;
	case 5:
#line 107 "src/nc_confx.rl"
	{
            weight = weight * 10 + ((*p) - '0');
        }
	break;
	case 6:
#line 111 "src/nc_confx.rl"
	{
            pnamelen = p - pname;
        }
	break;
	case 7:
#line 115 "src/nc_confx.rl"
	{
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }
	break;
	case 8:
#line 120 "src/nc_confx.rl"
	{
            namelen++;
        }
	break;
	case 9:
#line 124 "src/nc_confx.rl"
	{
            name = p;
        }
	break;
	case 10:
#line 128 "src/nc_confx.rl"
	{
            log_error("conf: invalid name");
            status = NC_ERROR;
        }
	break;
	case 11:
#line 133 "src/nc_confx.rl"
	{
            rstart = rstart * 10 + ((*p) - '0');
        }
	break;
	case 12:
#line 137 "src/nc_confx.rl"
	{
            rend = rend * 10 + ((*p) - '0');
        }
	break;
	case 13:
#line 141 "src/nc_confx.rl"
	{
            log_error("conf: invalid range");
            status = NC_ERROR;
        }
	break;
	case 14:
#line 146 "src/nc_confx.rl"
	{
            log_error("conf: invalid server conf");
            status = NC_ERROR;
        }
	break;
	case 15:
#line 151 "src/nc_confx.rl"
	{
            tag = p;
        }
	break;
	case 16:
#line 155 "src/nc_confx.rl"
	{
            taglen = (uint32_t)(p - tag);;
        }
	break;
	case 17:
#line 159 "src/nc_confx.rl"
	{
            log_error("conf: invalid tag");
            status = NC_ERROR;
        }
	break;
	case 18:
#line 164 "src/nc_confx.rl"
	{ flags = NC_SERVER_READABLE | NC_SERVER_WRITABLE; }
	break;
	case 19:
#line 165 "src/nc_confx.rl"
	{ flags = NC_SERVER_READABLE; }
	break;
	case 20:
#line 166 "src/nc_confx.rl"
	{ flags = NC_SERVER_WRITABLE; }
	break;
	case 21:
#line 167 "src/nc_confx.rl"
	{ flags = 0; }
	break;
#line 401 "src/nc_confx.c"
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
#line 88 "src/nc_confx.rl"
	{
            log_error("conf: invalid addr");
            status = NC_ERROR;
        }
	break;
	case 7:
#line 115 "src/nc_confx.rl"
	{
            log_error("conf: invalid weight");
            status = NC_ERROR;
        }
	break;
	case 13:
#line 141 "src/nc_confx.rl"
	{
            log_error("conf: invalid range");
            status = NC_ERROR;
        }
	break;
	case 16:
#line 155 "src/nc_confx.rl"
	{
            taglen = (uint32_t)(p - tag);;
        }
	break;
	case 17:
#line 159 "src/nc_confx.rl"
	{
            log_error("conf: invalid tag");
            status = NC_ERROR;
        }
	break;
#line 451 "src/nc_confx.c"
		}
	}
	}

	_out: {}
	}

#line 203 "src/nc_confx.rl"


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

    if (taglen > 0) {
       status = string_copy(&field->tag, tag, taglen);
       if (status != NC_OK) {
           return CONF_ERROR;
       }
       log_debug(LOG_VERB, "tag %.*s", taglen, tag);
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
