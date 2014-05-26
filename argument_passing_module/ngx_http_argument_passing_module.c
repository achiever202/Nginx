#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

static ngx_str_t first;
static ngx_str_t last;

typedef struct
{
	ngx_str_t first_name;
	ngx_str_t last_name;
}ngx_http_argument_passing_conf_t;

static char* ngx_http_argument_passing(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void* ngx_http_argument_passing_create_srv_conf(ngx_conf_t *cf);

static ngx_command_t ngx_http_argument_passing_commands[] = {
	{
		ngx_string("pass"),
		NGX_HTTP_LOC_CONF|NGX_CONF_TAKE2,
		ngx_http_argument_passing,
		0,
		0,
		NULL
	},

	ngx_null_command
};

ngx_http_module_t ngx_http_argument_passing_module_ctx = {
	NULL,
	NULL,

	NULL,
	NULL,

	ngx_http_argument_passing_create_srv_conf,
	NULL,

	NULL,
	NULL
};

ngx_module_t ngx_http_argument_passing_module = {
	NGX_MODULE_V1,
	&ngx_http_argument_passing_module_ctx,
	ngx_http_argument_passing_commands,
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

static void* ngx_http_argument_passing_create_srv_conf(ngx_conf_t *cf)
{
	ngx_http_argument_passing_conf_t *conf;
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_argument_passing_conf_t));

	return conf;
}

static ngx_int_t ngx_http_argument_passing_handler(ngx_http_request_t *r)
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

	//ngx_http_argument_passing_conf_t *cglcf;
	//cglcf = ngx_http_get_module_srv_conf(r, ngx_http_argument_passing_module);

	b->pos = first.data;
	b->last = first.data + first.len;

	b->memory = 1;
	b->last_buf = 1;

	char output[1024];
	sprintf(output, "Inside First=%s, Last=%s", first.data, last.data);
	ngx_write_stderr(output);

	r->headers_out.status = NGX_HTTP_OK;
	r->headers_out.content_length_n = first.len;
	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
		return rc;

	return ngx_http_output_filter(r, &out);
}

static char* ngx_http_argument_passing(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_core_loc_conf_t *clcf;
	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_argument_passing_handler;

	u_char *values = (u_char*)cf->args->elts;
	u_char* f = &values[0];
	u_char* l = &values[1];

	/*first.data = f->data;
	first.len = ngx_strlen(first.data);

	last.data = l->data;
	last.len = ngx_strlen(last.data);*/

	first.data = f;
	first.len = ngx_strlen(first.data);

	last.data = l;
	last.len = ngx_strlen(last.data);

	char output[1024];
	sprintf(output, "First=%s, Last=%s\n", first.data, last.data);
	ngx_write_stderr(output);
	return NGX_CONF_OK;
}
