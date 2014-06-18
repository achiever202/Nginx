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
typedef struct
{
	int n;
	char address[256][16];
	int port[256];
}ngx_http_as_hosts;

typedef struct
{
	aerospike *as;
	bool is_hosts_present;  // this is check if the hosts is same as prev. hosts
	ngx_str_t default_hosts;
	char default_namespace [40];
	ngx_http_as_hosts current_hosts; //store the current hosts to which as obj. is connected to
	bool connected;
	bool use_server_conf;
	
	ngx_pool_t *pool;
}ngx_http_as_conf_t;



//static u_char connected[] = "Connected to aerospike!";
static u_char not_connected[] = "Not connected to aerospike!";
/*static u_char connected_server[] = "Connected to server configuration!";
static u_char not_connected_server[] = "Could not connect to server configuration!";
static u_char connected_local[] = "Connected to local configuration!";
static u_char not_connected_local[] = "Could not connect to local configuration!";
static u_char not_entered[] = "Not entered the block";*/
static u_char put_success[] = "Put successful";
static u_char put_unsuccess[] = "Put not successful";



static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf);
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void* conf);
static char* ngx_http_as_use_srv_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_as_operate(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

bool ngx_http_as_operate_connect(ngx_http_request_t *r, ngx_http_as_conf_t *as_conf);
int ngx_http_as_utils_put(ngx_str_t url, aerospike *as);
bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts);
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts);
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts);
bool ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char *arg, char value[]);
void ngx_http_as_utils_replace(char * o_string, char * s_string, char * r_string);
bool ngx_http_as_utils_compare_prev_new_hosts(ngx_http_as_hosts current_hosts, ngx_http_as_hosts hosts);

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

static ngx_int_t ngx_http_as_operate_handler(ngx_http_request_t *r)
{
	//ngx_write_stderr("In as_operate handler\n");
	//ngx_write_stderr((char*)r->args.data);

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

	ngx_http_as_conf_t *as_conf;
	as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);

	if(as_conf->use_server_conf)
		as_conf = ngx_http_get_module_srv_conf(r, ngx_http_as_module);

	bool is_connected = ngx_http_as_operate_connect(r, as_conf);
	char operation[20];
	ngx_http_as_utils_get_parsed_url_arguement(r->args, "op",operation);

	//ngx_write_stderr((char*)is_connected);
	if(is_connected && strcmp(operation,"put")==0)
	{
		if(ngx_http_as_utils_put(r->args,as_conf->as))
		{
			b->pos = put_success;
			b->last = put_success + sizeof(put_success) - 1;

			r->headers_out.status = NGX_HTTP_OK;
			r->headers_out.content_length_n = sizeof(put_success)-1;
		}
		else
		{
			b->pos = put_unsuccess;
			b->last = put_unsuccess + sizeof(put_unsuccess) - 1;

			r->headers_out.status = NGX_HTTP_OK;
			r->headers_out.content_length_n = sizeof(put_unsuccess)-1;
		}
	}
	else if(is_connected && strcmp(operation,"get")==0)
	{

	}
	else
	{
		b->pos = not_connected;
		b->last = not_connected + sizeof(not_connected) - 1;

		r->headers_out.status = NGX_HTTP_OK;
		r->headers_out.content_length_n = sizeof(not_connected)-1;
	}

	b->memory = 1;
	b->last_buf = 1;

	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
		return rc;

	return ngx_http_output_filter(r, &out);


}

bool ngx_http_as_operate_connect(ngx_http_request_t *r, ngx_http_as_conf_t *as_conf)
{
	//ngx_write_stderr("In ngx_http_as_operate_connect\n");
	//ngx_write_stderr((char*)r->args.data);



	// hosts stores the hosts address and ports to iniitailise the cluster object with.
	// hosts_arrived_in_url checks whether the request contains the ip and ports.
	ngx_http_as_hosts hosts;
	bool hosts_arrived_in_url = false;

	ngx_write_stderr("writng url string: ");
	ngx_write_stderr((char*)r->args.data);
	ngx_write_stderr("\n");

	// getting the hosts string from the url.
	char hosts_string[1000] = "";
	ngx_http_as_utils_get_parsed_url_arguement(r->args, "hosts", hosts_string);

	ngx_write_stderr("writng hosts_string: ");
	ngx_write_stderr(hosts_string);
	ngx_write_stderr("\n");

	// if the url contains ip and ports, setting the hosts_string to true, and parsing the host string.
	if(strlen(hosts_string)>0)
	{
		hosts_arrived_in_url = true;
		ngx_http_as_utils_get_hosts(hosts_string, &hosts);
		//if first tme connect then copy the host
		if(as_conf->current_hosts.n==0)
		{
			as_conf->current_hosts = hosts;
		}
		as_conf->is_hosts_present = ngx_http_as_utils_compare_prev_new_hosts(as_conf->current_hosts, hosts);
	}


	// if hosts arrived in url, we need to connect to that addresses.
	if(hosts_arrived_in_url && !(as_conf->is_hosts_present))
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
		if(ngx_http_as_utils_connect(&(as_conf->as), hosts))
		{
			as_conf->connected = true;
			return true;
		}
	}
	else
	{
		// if using the default server hosts.
		// if the object is not yet connected, we need to initialize.
		if(!as_conf->connected)
		{
			ngx_http_as_utils_get_hosts((char*)as_conf->default_hosts.data, &hosts);
			if(as_conf->current_hosts.n==0)
			{
				as_conf->current_hosts = hosts;
			}
			if(ngx_http_as_utils_connect(&(as_conf->as), hosts))
			{
				as_conf->connected = true;
				return true;
			}
		}
		else
		{
			return true;
		}
	}
	return false;
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
bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts)
{
	// creating and initialising the as_config object.
	as_config cfg;
	as_config_init(&cfg);

	// adding the multiple ip and ports to the as_config object.
	ngx_http_as_utils_create_config(&cfg, hosts);

	// connecting to aerospike.
	*as = aerospike_new(&cfg);
	as_error err;
	if(aerospike_connect(*as, &err)!=AEROSPIKE_OK)
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

bool ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char* arg, char value[])
{
	char temp2[1000], temp3[1000];
	char temp_args[100][1000];
	int pos = 0, i;
	bool flag = false;
	bool is_str = false;
	int len = url.len;
	char url_string[len];

	if(len>0)
	{
		char *temp = strtok((char*)url.data, " ");
		strcpy(url_string, temp);
		while(temp!=NULL)
			temp = strtok(NULL, " ");

		ngx_write_stderr("writng space removed_string: ");
		ngx_write_stderr(url_string);
		ngx_write_stderr("\n");

		temp = strtok(url_string, "&");
		while(temp!=NULL)
		{
			strcpy(temp_args[pos], temp);
			pos++;

			temp = strtok(NULL, "&");
		}

		for(i=0; i<pos; i++)
		{
			temp = strtok(temp_args[i], "=");
			strcpy(temp2, temp);

			temp = strtok(NULL, "=");

			if(strcmp(temp2, arg)==0)
			{
				flag = 1;
				strcpy(temp3, temp);
				break;
			}
		}

		if(flag)
		{
			is_str = (temp3[0]=='%')?true:false;

			ngx_http_as_utils_replace(temp3, "%22", "\"");
			temp = strtok(temp3, "\"");
			strcpy(value, temp);
		
		}
	}
	return is_str;
}

int ngx_http_as_utils_put(ngx_str_t url, aerospike *as)
{
	ngx_write_stderr("In put function\n");


	//ngx_http_as_conf_t *as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);

	if(as==NULL)
		ngx_write_stderr("Shit! Its null\n");
	else
		ngx_write_stderr("Everythings fine mate!\n");

	char key[1000], namespace[40], set[100], bin[1000], value[1000];

	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);
	ngx_http_as_utils_get_parsed_url_arguement(url, "bin", bin);
	bool is_str = ngx_http_as_utils_get_parsed_url_arguement(url, "value", value);

	as_key put_key;
	as_key_init(&put_key, namespace, set, key);

	as_record rec;
	as_record_inita(&rec, 1);
	if(is_str)
	{
		as_record_set_str(&rec, bin, value);
	}
	else
	{
		int value_int = atoi(value);
		as_record_set_int64(&rec, bin, value_int);
	}

	as_error err;
	if(aerospike_key_put(as, &err, NULL, &put_key, &rec)!=AEROSPIKE_OK)
	{
		return 0;
	}
	else
		return 1;
}

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

 bool ngx_http_as_utils_compare_prev_new_hosts(ngx_http_as_hosts current_hosts, ngx_http_as_hosts hosts)
{
	if(current_hosts.n < hosts.n || current_hosts.n > hosts.n)
		return false;
	int i,j;
	bool flag = false;
	for(i=0;i<hosts.n;i++)
	{
		for(j=0;j<current_hosts.n;j++)
		{
			if((strcmp(current_hosts.address[j], hosts.address[i])==0))
			{
				if(current_hosts.port[j]==hosts.port[i])
					flag = true;
			}
		}
		if(!flag)
			break;
	}
	if(flag)
		return true;
	else
		return false;

}