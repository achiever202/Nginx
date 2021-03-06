#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

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

#define MAX_L 4096

// aerospike include ends.

/* This is the structure used for aerospike configurations.
 * as is the cluster object.
 * default_hosts holds the default ip address and ports to connect to, specified in conf file.
 * default_namespace is the default namespace to be used.
 * connected is a flag that keeps a check if the aerospike object is connected to the cluster.
 * use_server_conf stores, whether to use server configuration or local configuration.
 * pool is the conf pool, used for allocating memory during request handling.
 */
typedef struct
{
	aerospike *as;

	ngx_str_t default_hosts;
	char default_namespace [40];
	
	bool connected;
	bool use_server_conf;
	
	ngx_pool_t *pool;
}ngx_http_as_conf_t;

/* This is the structure that holds the ip addresses and ports, that as_config is to be initialised with.
 * n holds the number of ip:port pairs.
 * addresses holds the ip addresses.
 * port holds the ports for respective ip of the same index.
 */
typedef struct
{
	int n;
	char address[256][16];
	int port[256];
}ngx_http_as_hosts;

/* Functions to set up configurations */
static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf);
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf);

/* Functions for directives */
static char* ngx_http_as_use_srv_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_as_operate(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

/* Database operation functions called from as_operate */
void ngx_http_as_operate_connect(ngx_http_request_t *r, ngx_http_as_conf_t *as_conf, char response[]);
void ngx_http_as_operate_put(ngx_str_t url, aerospike *as, char response[]);
void ngx_http_as_operate_get(ngx_str_t url, aerospike *as, char response[]);
void ngx_http_as_operate_del(ngx_str_t url, aerospike *as, char response[]);

/* Utility functions */
bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts, char response[]);
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts);
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts);
bool ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char *arg, char value[]);
void ngx_http_as_utils_replace(char * o_string, char * s_string, char * r_string);
void ngx_http_as_utils_dump_record(as_record *p_rec, as_error err, char response[]);
void ngx_http_as_utils_dump_bin(const as_bin* p_bin, char response[]);
void ngx_http_as_utils_dump_error(as_error err, char response[], char* last_char);

/* This structure specifies the directives for the module.
 * as_connect - to store the default hosts for a local configuration.
 * as_use_srv_connect - to use server configuration instead of local configuration in a location.
 * as_operate - for various databse operations.
 */
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
		ngx_string("as_use_srv_conf"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
		ngx_http_as_use_srv_conf,
		0,
		0,
		NULL
	},

	{
		ngx_string("as_operate"),
		NGX_HTTP_LOC_CONF|NGX_HTTP_SRV_CONF|NGX_CONF_NOARGS,
		ngx_http_as_operate,
		0,
		0,
		NULL
	},

	ngx_null_command
};

/* Specifying the contexts of the module */
ngx_http_module_t ngx_http_as_module_ctx = {
	NULL,
	NULL,

	NULL,
	NULL,

	ngx_http_as_module_create_srv_conf,
	NULL,

	ngx_http_as_module_create_loc_conf,
	NULL
};

/* Declaring the module */
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
	ngx_http_as_conf_t *conf;

	// Allocating memory using the default nginx function pcalloc.
	// It takes care of deallocating memory later.
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_conf_t));

	// Setting all the flag variables for the cluster objects to false.
	// No cluster objects initialised.
	conf->as = NULL;
	conf->connected = false;
	conf->use_server_conf = true;
	conf->pool = cf->pool;

	return conf;
}

/* This function creates the location configuration for the module.
 * It allocates memory for the nngx_http_as_loc_conf_t object for the location.
 */
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf)
{
	// creating a location object and allocating memory.
	ngx_http_as_conf_t *conf;
	conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_as_conf_t));

	// setting default values.
	conf->as = NULL;
	conf->connected = false;
	conf->use_server_conf = false;
	conf->pool = cf->pool;

	return conf;
}

/* This function is the handler for the as_operate directive.
 * It processes the http requests, and performs various database operations.
 * For any request, first the connection is checked/made according to the parameters in the url.
 * Then, operations like get, put, del are handled.
 */
static ngx_int_t ngx_http_as_operate_handler(ngx_http_request_t *r)
{
	// Variables for sending back response.
	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;

	rc = ngx_http_discard_request_body(r);

	if(rc!=NGX_OK)
		return rc;

	// Initialising headers.
	r->headers_out.content_type_len = sizeof("text/html")-1;
	r->headers_out.content_type.len = sizeof("text/html")-1;
	r->headers_out.content_type.data = (u_char *)"text/html";

	// Allocating memory for the response buffer.
	b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
	if(b==NULL)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	// Adding it to the chain.
	out.buf = b;
	out.next = NULL;

	// as_configuration to be used for the operations.
	ngx_http_as_conf_t *as_conf;
	as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);

	// response string to be generated.
	char response[129000] = "\0";

	// If server configuration to be used, changing it to server configuration.
	if(as_conf->use_server_conf)
		as_conf = ngx_http_get_module_srv_conf(r, ngx_http_as_module);

	// connecting to aerospike.
	ngx_http_as_operate_connect(r, as_conf, response);

	// if the cluster object is connected.
	if(as_conf->connected)
	{
		char operation[10] = "";

		// getting the operation parameter from the url, get/put/del.
		ngx_http_as_utils_get_parsed_url_arguement(r->args, "op", operation);

		// for put operation.
		if(strcmp("put", operation)==0)
		{
			response[0] = '\0';
			ngx_http_as_operate_put(r->args, as_conf->as, response);
		}

		// for get operation.
		else if(strcmp("get", operation)==0)
		{
			response[0] = '\0';
			ngx_http_as_operate_get(r->args, as_conf->as, response);			
		}

		// for del operation.
		else if(strcmp("del", operation)==0)
		{
			response[0] = '\0';
			ngx_http_as_operate_del(r->args, as_conf->as, response);
		}
	}

	// copying the response string to the response buffer.
	b->pos = (u_char*)response;
	b->last = (u_char*)response + sizeof(response) - 1;

	// setting headers.
	r->headers_out.status = NGX_HTTP_OK;
	r->headers_out.content_length_n = sizeof(response)-1;

	b->memory = 1;
	b->last_buf = 1;

	// returning the reply.
	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
		return rc;

	return ngx_http_output_filter(r, &out);


}

/* This function is called to connect to the aerospike cluster.
 * It is called from the as_operate handler when a request is being processed.
 * If hosts are specified in the url, it connects to the new set of hosts.
 * Else, it check if the as object is connected to the default hosts specified, if any.
 * It then sets the connected flag as required.
 */
void ngx_http_as_operate_connect(ngx_http_request_t *r, ngx_http_as_conf_t *as_conf, char response[])
{
	// hosts stores the hosts address and ports to iniitailise the cluster object with.
	// hosts_arrived_in_url checks whether the request contains the ip and ports.
	ngx_http_as_hosts hosts;
	bool hosts_arrived_in_url = false;

	// getting the hosts string from the url.
	char hosts_string[1000] = "";
	ngx_http_as_utils_get_parsed_url_arguement(r->args, "hosts", hosts_string);

	// if the url contains ip and ports, setting the hosts_string to true, and parsing the host string.
	if(strlen(hosts_string)>0)
	{
		hosts_arrived_in_url = true;
		ngx_http_as_utils_get_hosts(hosts_string, &hosts);
	}

	// if hosts arrived in url, we need to connect to that addresses.
	if(hosts_arrived_in_url)
	{
		// if the object is previously connected, closing the connection.
		if(as_conf->connected)
		{
			as_error err;
			aerospike_close(as_conf->as, &err);
			aerospike_destroy(as_conf->as);

			as_conf->connected = false;
		}
			
		// connecting to new configurations.
		if(ngx_http_as_utils_connect(&(as_conf->as), hosts, response))
		{
			as_conf->connected = true;
		}
	}
	else
	{
		// if using the default server hosts.
		// if the object is not yet connected, we need to initialize.
		if(!as_conf->connected)
		{
			ngx_http_as_utils_get_hosts((char*)as_conf->default_hosts.data, &hosts);
			if(ngx_http_as_utils_connect(&(as_conf->as), hosts, response))
			{
				as_conf->connected = true;
			}
		}
	}
}

/* This function performs the put operation. */
void ngx_http_as_operate_put(ngx_str_t url, aerospike *as, char response[])
{
	// Case for debugging, if the as_object is null here.
	if(as==NULL)
	{
		strncat(response, "as object found null in as_operate_put().", strlen("as object found null in as_operate_put()."));
	}

	char key[1000], namespace[40], set[100], bin[1000], value[1000];

	// Obtaining namespace, set, key, bin and value from url.
	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);
	ngx_http_as_utils_get_parsed_url_arguement(url, "bin", bin);
	bool is_str = ngx_http_as_utils_get_parsed_url_arguement(url, "value", value);

	// Initialinsing the key.
	as_key put_key;
	as_key_init(&put_key, namespace, set, key);

	// Initialsing the record.
	as_record rec;
	as_record_inita(&rec, 1);

	// Checking for string/int bin.
	if(is_str)
	{
		as_record_set_str(&rec, bin, value);
	}
	else
	{
		int value_int = atoi(value);
		as_record_set_int64(&rec, bin, value_int);
	}

	// Performing put.
	as_error err;
	aerospike_key_put(as, &err, NULL, &put_key, &rec);
	
	// Starting the response string.
	strncat(response, "{\n", strlen("{\n"));
	ngx_http_as_utils_dump_error(err, response, "");
}

/* This function performs the get operation. */
void ngx_http_as_operate_get(ngx_str_t url, aerospike *as, char response[])
{
	// Case for debugging, if the as_object is null here.
	if(as==NULL)
	{
		strncat(response, "as object null in as_operate_get\n", strlen("as object null in as_operate_get\n"));
		return;
	}

	char key[1000], namespace[40], set[100];

	// Obtaining namespace, set, key from url.
	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);

	// Initialinsing the key.
	as_key get_key;
	as_key_init_str(&get_key, namespace, set, key);

	as_error err;
	as_record* p_rec = NULL;

	// Starting the json formatted string.
	strncat(response, "{\n", strlen("{\n"));

	// Read the (whole) test record from the database and generating response.
	if (aerospike_key_get(as, &err, NULL, &get_key, &p_rec) != AEROSPIKE_OK)
	{
		ngx_http_as_utils_dump_error(err, response, "");
	}
	else
	{
		ngx_http_as_utils_dump_error(err, response, ",");
		ngx_http_as_utils_dump_record(p_rec, err, response);
	}
}

/* This function performs the delete operation. */
void ngx_http_as_operate_del(ngx_str_t url, aerospike *as, char response[])
{
	// Case for debugging, if the as_object is null here.
	if(as==NULL)
	{
		ngx_write_stderr("as object null in as_operate_del\n");
		strncat(response, "as object null in as_operate_del\n", strlen("as object null in as_operate_del\n"));
		return;
	}

	char key[1000], namespace[40], set[100];

	// Obtaining namespace, set, key from url.
	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);

	as_key del_key;
	as_key_init_str(&del_key, namespace, set, key);

	// Starting the json formatted string.
	strncat(response, "{\n", strlen("{\n"));

	as_error err;

	// Delete the (whole) test record from the database.
	aerospike_key_remove(as, &err, NULL, &del_key);
	
	ngx_http_as_utils_dump_error(err, response, "");
}



/* This function sets up the as_connect directive.
 * It takes one arguements, which is the default hosts string, of the form, 127.0.0.1:3000,127.0.0.1:4000
 */
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	// Storing the arguemnts of the directive.
	ngx_str_t *arguments = cf->args->elts;

	// Acessing the server/local configuration.
	ngx_http_as_conf_t *as_conf;

	// Checking for the context, i.e. server/local.
	if(cf->cmd_type==NGX_HTTP_SRV_CONF)
		as_conf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_as_module);
	else
		as_conf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_as_module);

	// Stroing the default hosts provided in the arguement for the configuration.
	as_conf->default_hosts.data = arguments[1].data;
	as_conf->default_hosts.len = ngx_strlen(as_conf->default_hosts.data);

	return NGX_CONF_OK;
}

/* This funnction sets the flag for the local configuratation to use the server context object. */
static char* ngx_http_as_use_srv_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_as_conf_t *as_conf;
	as_conf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_as_module);
	as_conf->use_server_conf = true;
	return NGX_CONF_OK;
}

/* This function sets the handler for the as_operate_directive. */
static char* ngx_http_as_operate(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_http_core_loc_conf_t *clcf;
	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_as_operate_handler;
	return NGX_CONF_OK;
}

/* This function accepts an aerospike object, and the hosts to be connected.
 * It then created the connected to the cluster.
 * If the connection is succesful, it returns true, else false.
 */
bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts, char response[])
{
	// creating and initialising the as_config object.
	as_config cfg;
	as_config_init(&cfg);

	// adding the multiple ip and ports to the as_config object.
	ngx_http_as_utils_create_config(&cfg, hosts);

	// connecting to aerospike.
	*as = aerospike_new(&cfg);
	as_error err;

	// starting response
	strncat(response, "{\n", strlen("{\n"));

	if(aerospike_connect(*as, &err)!=AEROSPIKE_OK)
	{
		ngx_http_as_utils_dump_error(err, response, "");
		return false;
	}

	ngx_http_as_utils_dump_error(err, response, "");
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

/* This function takes as argument a character array of ip ports separeated by ";" which in turn are separated by ",".
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

		ngx_write_stderr("writng address: ");
		ngx_write_stderr(temp);
		ngx_write_stderr("\n");

		temp = strtok(NULL, ":");

		ngx_write_stderr("writng port: ");
		ngx_write_stderr(temp);
		ngx_write_stderr("\n");

		hosts->port[i] = atoi(temp);
	}
}

/* This function parses the url, for the value of a particular argument.
 * It takes as parameter three arguments, ngx_str_t, char*, char*.
 * The first argument is the url to be parsed.
 * The second argument is the name of the argument in the url whose value is to be found.
 * The third argument is the array where the value is to be stored.
 * It returns a bool, if the value is a string/int.
 */
bool ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char* arg, char value[])
{
	// variables to be used.
	char temp2[1000], temp3[1000];
	char temp_args[100][1000];
	int pos = 0, i;
	bool flag = false;
	bool is_str = false;
	int len = url.len;
	char url_string[len];

	// if there are arguments in url.
	if(len>0)
	{
		// parsing to remove the space at the end of the url.
		char *temp = strtok((char*)url.data, " ");

		// copying the part of the url with arguments without a space, in a variiable.
		strcpy(url_string, temp);

		while(temp!=NULL)
			temp = strtok(NULL, " ");

		// parsing the url with "&", i.e. for different arguments, and storing each name value pair.
		temp = strtok(url_string, "&");
		while(temp!=NULL)
		{
			strcpy(temp_args[pos], temp);
			pos++;

			temp = strtok(NULL, "&");
		}

		// for each name value pair, parsing the pair, and checking for the required arguments, specified by arg.
		for(i=0; i<pos; i++)
		{
			temp = strtok(temp_args[i], "=");
			strcpy(temp2, temp);

			temp = strtok(NULL, "=");

			// if the name-value pair found, copying the value, and setting the flag.
			if(strcmp(temp2, arg)==0)
			{
				flag = 1;
				strcpy(temp3, temp);
				break;
			}
		}

		// if name value pair found.
		if(flag)
		{
			// checking for string/int
			is_str = (temp3[0]=='%')?true:false;

			// replacing %22 with "\""
			ngx_http_as_utils_replace(temp3, "%22", "\"");
				
			temp = strtok(temp3, "\"");

			// if the value is not null, copying it into value.
			if(temp!=NULL)
				strcpy(value, temp);
		}
	}

	return is_str;
}

/* This function replaces all occurances of a sequences of characters in a string with other. */
void ngx_http_as_utils_replace(char * o_string, char * s_string, char * r_string)
{
      //a buffer variable to do all replace things
      char buffer[MAX_L];
      //to store the pointer returned from strstr
      char * ch;
 
      //first exit condition
      if(!(ch = strstr(o_string, s_string)))
              return;
 
      //copy all the content to buffer before the first occurrence of the search string
      strncpy(buffer, o_string, ch-o_string);
 
      //prepare the buffer for appending by adding a null to the end of it
      buffer[ch-o_string] = 0;
 
      //append using sprintf function
      sprintf(buffer+(ch - o_string), "%s%s", r_string, ch + strlen(s_string));
 
      //empty o_string for copying
      o_string[0] = 0;
      strcpy(o_string, buffer);
      //pass recursively to replace other occurrences
      return ngx_http_as_utils_replace(o_string, s_string, r_string);
 }

/* This function formats a bin as a json in the response string.
 * The first parameter is the bin to be formatted.
 * The second parameter is the response string.
 */
void ngx_http_as_utils_dump_bin(const as_bin* p_bin, char response[])
{
	// if the bin is null, writing to the log file.
 	if (! p_bin)
 	{
		ngx_write_stderr("Bin passed to dump_bin is null.\n");
		return;
	}

	// obtaing the value of bin as json formatted string.
	char* val_as_str = as_val_tostring(as_bin_get_value(p_bin));

	// writing the bin name, and the bin value into the response string.
	strncat(response, "\t\t", strlen("\t\t"));
	strncat(response, "\"", strlen("\""));
	strncat(response, as_bin_get_name(p_bin), strlen(as_bin_get_name(p_bin)));
	strncat(response, "\"", strlen("\""));
	strncat(response, ":", strlen(":"));
	strncat(response, val_as_str, strlen(val_as_str));

	free(val_as_str);
 }

/* This function creates the json formatted string for a record.
 * The first parameter is the record, whose json formatting is to be done.
 * The second parameter is a character array, which stores the json of the record.
 */
void ngx_http_as_utils_dump_record(as_record *p_rec, as_error err, char response[])
{
	// If the record is null, write to the log file.
	if (! p_rec) {
		ngx_write_stderr("The record object passed to dump_record is null.");
		return;
	}

	// Obtaining the number of bins in the record.
	uint16_t num_bins = as_record_numbins(p_rec);

	// Starting metadata block.
	strncat(response, "\t\"Metadata\":\n\t{\n", strlen("\t\"Meta-data\":\n\t{\n"));

	// Appending the number of bins.
	char temp_json_string[100];
	sprintf(temp_json_string, "%d", (int)num_bins);
	strncat(response, "\t\t\"Num_bins\": ", strlen("\t\t\"Num_bins\": "));
	strncat(response, temp_json_string, strlen(temp_json_string));
	strncat(response, ",\n", strlen(",\n"));

	// Appending the generation of the record
	sprintf(temp_json_string, "%d", (int)p_rec->gen);
	strncat(response, "\t\t\"Generation\": ", strlen("\t\t\"Generation\": "));
	strncat(response, temp_json_string, strlen(temp_json_string));
	strncat(response, ",\n", strlen(",\n"));

	// appending the ttl.
	sprintf(temp_json_string, "%d", (int)p_rec->ttl);
	strncat(response, "\t\t\"Ttl\": ", strlen("\t\t\"Ttl\": "));
	strncat(response, temp_json_string, strlen(temp_json_string));
	strncat(response, "\n", strlen("\n"));

	// Ending metadata.
	strncat(response, "\t},\n", strlen("\t},\n"));

	// Starting the bins block
	strncat(response, "\t\"Bins\":\n\t{\n", strlen("\t\"Bins\":\n\t{\n"));
	as_record_iterator it;
	as_record_iterator_init(&it, p_rec);

	// for json formatting, excluding the first bin.
	int ctr = 0;

	// for each bin in the record.
	while (as_record_iterator_has_next(&it))
	{

		// print "," after each record except the last one.
		if(ctr)
			strncat(response, ",\n", strlen(",\n"));

		// format the bin as json.
		ngx_http_as_utils_dump_bin(as_record_iterator_next(&it), response);
		ctr = 1;
	}

	// not appending "," at the end of last bin.
	strncat(response, "\n", strlen("\n"));

	as_record_iterator_destroy(&it);

	// Ending bin block
	strncat(response, "\t}\n", strlen("\t},\n"));

	// Ending string
	strncat(response, "}", strlen("}"));
 }

/* This function creates the json formatted string for an error.
 * The first parameter is the error object, whose json formatting is to be done.
 * The second parameter is a character array, which stores the json of the record.
 */
 void ngx_http_as_utils_dump_error(as_error err, char response[], char* last_char)
 {
 	//Starting the error block.
 	strncat(response, "\t\"Error\":\n\t{\n", strlen("\t\"Error\":\n\t{\n"));

 	// Adding the status code to the json string.
 	char status_code[5];
 	sprintf(status_code, "%d", err.code);
 	strncat(response, "\t\t\"Code\":", strlen("\t\t\"Code\":"));
 	strncat(response, status_code, strlen(status_code));
 	strncat(response, ",\n", strlen(",\n"));

 	// Adding the message to the json string.
 	strncat(response, "\t\t\"Message\":\"", strlen("\t\t\"Message\":\""));
 	strncat(response, err.message, strlen(err.message));
 	strncat(response, "\",\n", strlen("\",\n"));

 	// Adding the funtion where the error occured.
 	strncat(response, "\t\t\"Function\":\"", strlen("\t\t\"Function\":\""));
 	if(err.func==NULL)
 		strncat(response, "Null", strlen("Null"));
 	else
 		strncat(response, err.func, strlen(err.func));
 	strncat(response, "\",\n", strlen("\",\n"));

 	// Adding the file where the error occured.
 	strncat(response, "\t\t\"File\":\"", strlen("\t\t\"File\":\""));
 	if(err.func==NULL)
 		strncat(response, "Null", strlen("Null"));
 	else
 		strncat(response, err.file, strlen(err.file));
 	strncat(response, "\",\n", strlen("\",\n"));

 	// Adding the line where the error occured.
 	strncat(response, "\t\t\"Line\":", strlen("\t\t\"Line\":"));
 	if(err.func==NULL)
 		strncat(response, "\"Null\"", strlen("\"Null\""));
 	else
 	{
 		char line[10000] = "";
 		sprintf(line, "%d", (int)err.line);
 		strncat(response, line, strlen(line));
 	}
 	strncat(response, "\n", strlen("\n"));

 	// Ending the error block
 	if(strcmp(last_char, ",")==0)
 		strncat(response, "\t},\n", strlen("\t},\n"));
 	else
 		strncat(response, "\t}\n}", strlen("\t}\n}"));
 }