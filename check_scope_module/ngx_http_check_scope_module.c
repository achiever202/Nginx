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
	ngx_uint_t here;
}ngx_http_check_scope_conf_t;

u_char one[] = "1", two[] = "2", three[] = "3", zero[] = "0";

static char* ngx_http_check_scope(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void* ngx_http_check_scope_create_srv_conf(ngx_conf_t *cf);
//static void* ngx_http_check_scope_create_loc_conf(ngx_conf_t *cf);
//static char* ngx_http_check_scope_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);



static ngx_command_t ngx_http_check_scope_commands[] = {
	{
		ngx_string("check_scope"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
		ngx_http_check_scope,
		0,
		0,
		NULL
	},

	ngx_null_command
};

ngx_http_module_t ngx_http_check_scope_module_ctx = {
	NULL,
	NULL,

	NULL,
	NULL,

	ngx_http_check_scope_create_srv_conf,
	NULL,

	NULL,
	NULL
};

ngx_module_t ngx_http_check_scope_module = {
	NGX_MODULE_V1,
	&ngx_http_check_scope_module_ctx,
	ngx_http_check_scope_commands,
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

static void* ngx_http_check_scope_create_srv_conf(ngx_conf_t *cf)
{
	ngx_http_check_scope_conf_t *conf;
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_check_scope_conf_t));

	conf->here = 0;
	return conf;
}

/*static void* ngx_http_check_scope_create_loc_conf(ngx_conf_t *cf)
{
	ngx_http_check_scope_conf_t *conf;
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_check_scope_conf_t));
	return conf;
}

static char* ngx_http_check_scope_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	ngx_http_check_scope_conf_t *prev = parent;
	ngx_http_check_scope_conf_t *conf = child;

	conf->here = prev->here;
	return NGX_CONF_OK;
}*/

static ngx_int_t ngx_http_check_scope_handler(ngx_http_request_t *r)
{
	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;

	rc = ngx_http_discard_request_body(r);

	if(rc!=NGX_OK)
		return rc;

	r->headers_out.content_type_len = sizeof("text/html")-1;
	r->headers_out.content_type.len = sizeof("text/html")-1;
	r->headers_out.content_type.data = (u_char *)"text/html";

	b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
	if(b==NULL)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	out.buf = b;
	out.next = NULL;

	ngx_http_check_scope_conf_t *cglcf;
	cglcf = ngx_http_get_module_srv_conf(r, ngx_http_check_scope_module);

	cglcf->here = cglcf->here + 1;

	if(cglcf->here==1)
	{
		b->pos = one;
		b->last = one + sizeof(one)-1;
	}
	else if(cglcf->here==2)
	{
		b->pos = two;
		b->last = two + sizeof(two)-1;
	}
	else if(cglcf->here==3)
	{
		b->pos = three;
		b->last = three + sizeof(three)-1;
		cglcf->here = 0;
	}
	else
	{
		b->pos = zero;
		b->last = zero + sizeof(zero)-1;
	}

	b->memory = 1;
	b->last_buf = 1;


	r->headers_out.status = NGX_HTTP_OK;
	r->headers_out.content_length_n = sizeof(one)-1;
	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
		return rc;

	return ngx_http_output_filter(r, &out);
}

static char* ngx_http_check_scope(ngx_conf_t *cf, ngx_command_t *cmd, void* conf)
{
	ngx_http_core_loc_conf_t *clcf;
	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_check_scope_handler;
	return NGX_CONF_OK;
}
