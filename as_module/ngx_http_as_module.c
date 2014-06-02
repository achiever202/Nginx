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
	aerospike *as;
	bool connected;
}ngx_http_as_srv_conf_t;

typedef struct
{
	aerospike *as;
	bool connected;
	bool is_server_copied;
}ngx_http_as_loc_conf_t;

typedef struct
{
	int n;
	char address[256][16];
	int port[256];
}ngx_http_as_hosts;

static ngx_str_t host;
static u_char connected[] = "Connected to aerospike!";
static u_char not_connected[] = "Not connected to aerospike!";

static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf);
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_as_module_merge_loc_conf(ngx_conf_t *cf, void* parent, void* child);
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void* conf);
static char*ngx_http_as_connected(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

bool ngx_http_as_utils_connect(aerospike *as, ngx_http_as_hosts hosts);
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts);
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts);

static ngx_command_t ngx_http_as_commands[] = {
	{
		ngx_string("as_connect"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_TAKE1,
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

	ngx_http_as_module_create_loc_conf,
	ngx_http_as_module_merge_loc_conf
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
 */
static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf)
{

	// The configuration pointer to the structure.
	ngx_http_as_srv_conf_t *conf;

	// Allocating memory using the default nginx function pcalloc.
	// It takes care of deallocating memory later.
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_srv_conf_t));

	// Setting all the flag variables for the cluster objects to false.
	// No cluster objects initialised.
	conf->as = NULL;
	conf->connected = false;

	return conf;
}

/* This function creates the location configuration for the module.
 * It allocates memory for the nngx_http_as_loc_conf_t object for the location.
 */
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf)
{
	// creating a location object and allocating memory.
	ngx_http_as_loc_conf_t *conf;
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_loc_conf_t));

	// setting default values.
	conf->as = NULL;
	conf->connected = false;
	conf->is_server_copied = false;

	return conf;
}

/* This function merges the server and location configurations.
 * If the location object is already initialised, it does nothing.
 * If the object is not initialised, it copies the server configuration and sets the flags.
 */
static char* ngx_http_as_module_merge_loc_conf(ngx_conf_t *cf, void* parent, void* child)
{
	// server context object.
	ngx_http_as_srv_conf_t *previous = parent;

	// location context object.
	ngx_http_as_loc_conf_t *conf = child;

	// If the location object is not initialised, copy the server configuration.
	if(conf->as==NULL)
	{
		conf->as = previous->as;
		conf->connected = previous->connected;
		conf->is_server_copied = true;

	}
	
	return NGX_CONF_OK;
}

/* This function is called when the command as_connect is called.
 * It sets the handler for the command.
 * It also takes in the arguments ip and port and stoers it into the ngx_str_t variables, host_ip and host_port respectively.
 */
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	// Dereferencing the arguments array from void* to ngx_str_t*.
	ngx_str_t *arguments = cf->args->elts;

	// stroing the argument passed in an ngx_str_t variable
	host.data = arguments[1].data;
	host.len = ngx_strlen(host.data);

	// parsing different hosts from the argument.
	ngx_http_as_hosts hosts;
	ngx_http_as_utils_get_hosts((char*)host.data, &hosts);

	if(cf->cmd_type==NGX_HTTP_SRV_CONF)
	{
		ngx_http_as_srv_conf_t *as_conf;
		as_conf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_as_module);

		as_conf->as = ngx_pcalloc(cf->pool, sizeof(aerospike));
		if(ngx_http_as_utils_connect(as_conf->as, hosts))
		{
			as_conf->connected = true;
		}
	}
	else
	{
		ngx_http_as_loc_conf_t *as_conf;
		as_conf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_as_module);
		
		as_conf->as = ngx_pcalloc(cf->pool, sizeof(aerospike));
		if(ngx_http_as_utils_connect(as_conf->as, hosts))
		{
			as_conf->connected = true;
		}
		as_conf->is_server_copied = false;
	}

	return NGX_CONF_OK;
}

static ngx_int_t ngx_http_as_connected_handler(ngx_http_request_t *r)
{
	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;

	ngx_http_as_loc_conf_t *as_conf;
	as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);

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

/* This function accepts an aerospike object, and the hosts to be connected.
 * It then created the connected to the cluster.
 * If the connection is succesful, it returns true, else false.
 */
bool ngx_http_as_utils_connect(aerospike *as, ngx_http_as_hosts hosts)
{
	// creating and initialising the as_config object.
	as_config cfg;
	as_config_init(&cfg);

	// adding the multiple ip and ports to the as_config object.
	ngx_http_as_utils_create_config(&cfg, hosts);

	// connecting to aerospike.
	aerospike_init(as, &cfg);
	as_error err;
	if(aerospike_connect(as, &err)!=AEROSPIKE_OK)
		return false;

	return true;
}

/* This function adds the different ip and ports, to the as_config object.*/
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts)
{
	int i;
	for(i=0; i<hosts.n; i++)
	{
		cfg->hosts[i].addr = hosts.address[i];
		cfg->hosts[i].port = hosts.port[i];
	}
}

/*This function takes as argument a character array of ip ports separeated by ";" which in turn are separated by ",".
 * It parses them into individual host ips and ports.
 */
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts)
{
	// temp_hosts stores the string combinations of host ip and port, merged using a semicolon.
	// for eg, 127.0.0.1:3000
	char temp_hosts[256][30];

	// pos stores the number of host ip and ports obtained. Set to 0.
	int pos = 0, i;

	// splitting up the different hosts, separated using ",".
	char *temp = strtok(arg, ",");
	while(temp!=NULL)
	{
		// copying the current host ip and port string into the temp_host array.
		strcpy(temp_hosts[pos], temp);
		pos++;

		temp = strtok(NULL, ",");
	}

	// setting the number of hosts as the value obtained above.
	hosts->n = pos;

	// For each host, separating the address and port.
	for(i=0; i<pos; i++)
	{
		temp = strtok(temp_hosts[i], ":");
		strcpy(hosts->address[i], temp);

		temp = strtok(NULL, ":");
		hosts->port[i] = atoi(temp);
	}
}
