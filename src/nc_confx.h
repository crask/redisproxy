#ifndef _NC_CONFX_H_
#define _NC_CONFX_H_

char *conf_add_server(struct conf *cf, struct command *cmd, void *conf);

void conf_server_init(struct conf_server *cs);
void conf_server_deinit(struct conf_server *cs);

#endif
