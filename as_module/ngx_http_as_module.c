#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

// aerospike includes.
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_val.h>
#include <aerospike/as_policy.h>

// aerospike include ends.

typedef struct
{
	aerospike as;
	bool connected;
}ngx_http_as_conf_t;

static ngx_str_t host_ip, host_port;
static u_char connected[] = "Connected to aerospike!";
static u_char not_connected[] = "Not connected to aerospike!";

static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf);
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void* conf);
static char*ngx_http_as_connected(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static ngx_command_t ngx_http_as_commands[] = {
	{
		ngx_string("as_connect"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE3,
		ngx_http_as_connect,
		0,
		0,
		NULL
	},

	{
		ngx_string("as_connected"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
		ngx_http_as_connected,
		0,
		0,
		NULL
	},

	ngx_null_command
};

ngx_http_module_t ngx_http_as_module_ctx = {
	NULL,
	NULL,

	NULL,
	NULL,

	ngx_http_as_module_create_srv_conf,
	NULL,

	NULL,
	NULL
};

ngx_module_t ngx_http_as_module = {
	NGX_MODULE_V1,
	&ngx_http_as_module_ctx,
	ngx_http_as_commands,
	NGX_HTTP_MODULE,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NGX_MODULE_V1_PADDING
};

/* This function creates the server configuration of the aersopike moodules.
 * It allocates memory for the ngx_http_as_conf_t structre.
 * Also, it sets the flags for all the 32 cluster objects as false, signifying that there are no cluster objects initialized.
 */
static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf)
{
	// The configuration pointer to the structure.
	ngx_http_as_conf_t *conf;

	// Allocating memory using the default nginx function pcalloc.
	// It takes care of deallocating memory later.
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_conf_t));

	// Setting all the flag variables for the cluster objects to false.
	// No cluster objects initialised.
	conf->connected = false;

	return conf;
}

/* This function is called when the command as_connect is called.
 * It sets the handler for the command.
 * It also takes in the arguments ip and port and stoers it into the ngx_str_t variables, host_ip and host_port respectively.
 */
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	// Dereferencing the arguments array from void* to ngx_str_t*.
	ngx_str_t *arguments = cf->args->elts;

	// Stroing the ip in host_ip.
	host_ip.data = arguments[1].data;
	host_ip.len = ngx_strlen(host_ip.data);

	// Stroing the port in host_port.
	host_port.data = arguments[2].data;
	host_ip.len = ngx_strlen(host_port.data);

	ngx_http_as_conf_t *as_conf;

	as_conf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_as_module);
	as_config cfg;

	as_config_init(&cfg);

	cfg.hosts[0].addr = (char*)host_ip.data;
	cfg.hosts[0].port = atoi((char*)host_port.data);

	aerospike_init(&(as_conf->as), &cfg);

	as_error err;

	if(aerospike_connect(&(as_conf->as), &err)==AEROSPIKE_OK)
	{
		as_conf->connected = true;
	}

	return NGX_CONF_OK;
}

static ngx_int_t ngx_http_as_connected_handler(ngx_http_request_t *r)
{
	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;

	ngx_http_as_conf_t *as_conf;
	as_conf = ngx_http_get_module_srv_conf(r, ngx_http_as_module);

	rc = ngx_http_discard_request_body(r);

	if(rc!=NGX_OK)
	{
		return rc;
	}

	r->headers_out.content_type_len = sizeof("text/html")-1;
	r->headers_out.content_type.len = sizeof("text/html")-1;
	r->headers_out.content_type.data = (u_char *)"text/html";

	b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
	if(b==NULL)
	return NGX_HTTP_INTERNAL_SERVER_ERROR;

	out.buf = b;
	out.next = NULL;

	if(as_conf->connected)
	{
		b->pos = connected;
		b->last = connected + sizeof(connected) - 1;

		r->headers_out.content_length_n = sizeof(connected)-1;
	}
	else
	{
		b->pos = not_connected;
		b->last = connected + sizeof(not_connected) - 1;

		r->headers_out.content_length_n = sizeof(not_connected)-1;
	}

	b->memory = 1;
	b->last_buf = 1;

	r->headers_out.status = NGX_HTTP_OK;
	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
	return rc;

	return ngx_http_output_filter(r, &out);
}

static char* ngx_http_as_connected(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
		ngx_http_core_loc_conf_t *clcf;
		clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
		clcf->handler = ngx_http_as_connected_handler;
		return NGX_CONF_OK;
}
