#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#define MAX_L 4096
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>

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

//testing code
void *print_message_function( void *ptr ){
char *message;
char output[1024];
   // pthread_id   tid;
   // tid = pthread_getthreadid_np();
	//	FILE *fp;

   
   
     message = (char *) ptr;
     while(1){
     	/*fp = fopen("/home/freakode0/test.txt", "w+");
     	fprintf(fp, "THis is a test...%ld",syscall(SYS_gettid));
     	fclose(fp);*/
     	//sleep(2);
    sprintf(output,"%s...%ld...%ld\n", message,syscall(SYS_gettid),pthread_self());
	ngx_write_stderr(output);
	sleep(5);
	//sprintf(output,"Exiting\n");
	//ngx_write_stderr(output);
	//return NULL;
     }

}
//////
void  test(){
pthread_t thread1, thread2;
     const char *message1 = "Thread 1";
     const char *message2 = "Thread 2";
     int  iret1, iret2;

    

     iret1 = pthread_create( &thread1, NULL, print_message_function, (void*) message1);
     if(iret1)
     {
         fprintf(stderr,"Error - pthread_create() return code: %d\n",iret1);
         exit(EXIT_FAILURE);
     }

     //int a = pthread_detach(thread1);

     iret2 = pthread_create( &thread2, NULL, print_message_function, (void*) message2);
     if(iret2)
     {
         fprintf(stderr,"Error - pthread_create() return code: %d\n",iret2);
         exit(EXIT_FAILURE);
     }
/*  	char o[1024];
  	sprintf(o,"pthread_create for thread 1 returns:%d\n",iret1);
  	ngx_write_stderr(o);
  	sprintf(o,"pthread_create for thread 2 returns:%d\n",iret2);
  	ngx_write_stderr(o);*/
     
     	//exit(EXIT_SUCCESS);

}


//testing code ends here



/*			TODO : 
we need to check if hosts provided in the url has already been
discovered by aerospike cluster or not.If it has been discoverd 
then we need not close and again connect to cluster.
*/

/*    Already Done:
	Right now we are jst checking if the hosts in the url has is same as the 
	previous hosts that we are connected to or not. If yes, then we do nothing.

*/


typedef struct
{
	int n;
	char address[256][16];
	int port[256];
}ngx_http_as_hosts;


typedef struct
{
	aerospike *as;

	ngx_str_t default_hosts;
	bool is_hosts_present;   // this is check if the hosts is same as prev. hosts
	bool connected;
	bool use_server_conf;
	ngx_http_as_hosts current_hosts; //store the current hosts to which as obj. is connected to
	ngx_pool_t *pool;
}ngx_http_as_conf_t;



//static u_char connected[] = "Connected to aerospike!";
//static u_char not_connected[] = "Not connected to aerospike!";
static u_char connected_server[] = "Connected to server configuration!";
static u_char not_connected_server[] = "Could not connect to server configuration!";
static u_char connected_local[] = "Connected to local configuration!";
static u_char not_connected_local[] = "Could not connect to local configuration!";
static u_char put_success[] = "Put successful";
//static u_char put_not_success[] = "Put not successful";


static void* ngx_http_as_module_create_srv_conf(ngx_conf_t *cf);
static void* ngx_http_as_module_create_loc_conf(ngx_conf_t *cf);
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void* conf);
//static char*ngx_http_as_connected(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static char* ngx_http_as_use_srv_conf(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

bool ngx_http_as_utils_connect(aerospike **as, ngx_http_as_hosts hosts);
void ngx_http_as_utils_create_config(as_config *cfg, ngx_http_as_hosts hosts);
void ngx_http_as_utils_get_hosts(char *arg, ngx_http_as_hosts *hosts);
void ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char *arg, char value[]);
bool ngx_http_as_utils_compare_prev_new_hosts(ngx_http_as_hosts current_hosts, ngx_http_as_hosts hosts);
void replace(char * o_string, char * s_string, char * r_string);
 int ngx_http_as_utils_put(ngx_http_request_t *r, ngx_str_t url, aerospike *as);


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
	conf->is_hosts_present = false;
	conf->use_server_conf = true;
	conf->pool = cf->pool;
	conf->current_hosts.n = 0;
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
	conf->is_hosts_present = false;
	conf->use_server_conf = false;
	conf->pool = cf->pool;

	return conf;
}

static ngx_int_t ngx_http_as_connect_handler(ngx_http_request_t *r)
{
	ngx_http_as_conf_t *as_conf;
	bool connection_made = false;
	bool using_server_configuration = false;
	as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);
	if(as_conf->use_server_conf)
	{
		using_server_configuration = true;
		as_conf = ngx_http_get_module_srv_conf(r, ngx_http_as_module);
	}

	// hosts stores the hosts address and ports to iniitailise the cluster object with.
	// hosts_arrived_in_url checks whether the request contains the ip and ports.
	ngx_http_as_hosts hosts;
	bool hosts_arrived_in_url = false;

	// request processing variables.
	ngx_int_t rc;
	ngx_buf_t *b;
	ngx_chain_t out;

	// setting the reply headers.
	r->headers_out.content_type_len = sizeof("text/html")-1;
	r->headers_out.content_type.len = sizeof("text/html")-1;
	r->headers_out.content_type.data = (u_char *)"text/html";

	// allocating memory for the buffer.
	b = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
	if(b==NULL)
		return NGX_HTTP_INTERNAL_SERVER_ERROR;

	// linking with the output chain.
	out.buf = b;
	out.next = NULL;
	//ngx_write_stderr((char *)r->args.data);
	// getting the hosts string from the url.
	char hosts_string[1000] = "";
	ngx_http_as_utils_get_parsed_url_arguement(r->args, "hosts", hosts_string);
	// if the url contains ip and ports, setting the hosts_string to true, and parsing the host string.
	if(strlen(hosts_string)>0)
	{
		hosts_arrived_in_url = true;
		ngx_http_as_utils_get_hosts(hosts_string, &hosts);
		if(as_conf->current_hosts.n==0)
		{
			as_conf->current_hosts = hosts;
		}
		as_conf->is_hosts_present = ngx_http_as_utils_compare_prev_new_hosts(as_conf->current_hosts, hosts);
	}




	// if hosts arrived in url and it is diff from prev. hosts, we need to connect to that addresses.
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
			connection_made = true;
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
				connection_made = true;
			}
		}
		else
		{
			connection_made = true;
		}
	}
	if(ngx_http_as_utils_put(r, r->args,as_conf->as))
	{
		b->pos = put_success;
		b->last = put_success + sizeof(put_success)-1;
		r->headers_out.content_length_n = sizeof(put_success)-1;
	}
	else if(using_server_configuration && connection_made)
	{
		b->pos = connected_server;
		b->last = connected_server + sizeof(connected_server) - 1;
		r->headers_out.content_length_n = sizeof(connected_server) - 1;
	}
	else if(using_server_configuration)
	{
		b->pos = not_connected_server;
		b->last = not_connected_server + sizeof(not_connected_server) - 1;
		r->headers_out.content_length_n = sizeof(not_connected_server) - 1;
	}
	else if(connection_made)
	{
		b->pos = connected_local;
		b->last = connected_local + sizeof(connected_local) - 1;
		r->headers_out.content_length_n = sizeof(connected_local) - 1;
	}
	else
	{
		b->pos = not_connected_local;
		b->last = not_connected_local + sizeof(not_connected_local) - 1;
		r->headers_out.content_length_n = sizeof(not_connected_local) - 1;
	}

	// sending back the reply.
	b->memory = 1;
	b->last_buf = 1;

	r->headers_out.status = NGX_HTTP_OK;
	rc = ngx_http_send_header(r);

	if(rc==NGX_ERROR || rc>NGX_OK || r->header_only)
	return rc;

	return ngx_http_output_filter(r, &out);
}

/* This function sets up the as_connect directive.
 * It takes one arguements, which is the default hosts string, of the form, 127.0.0.1:3000,127.0.0.1:4000
 */
static char* ngx_http_as_connect(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	// Storing the arguemnts of the directive.
	ngx_str_t *arguments = cf->args->elts;

	// Accesing the core configuration and setting up the handler.
	ngx_http_core_loc_conf_t *clcf;
	clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
	clcf->handler = ngx_http_as_connect_handler;

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

		temp = strtok(NULL, ":");
		hosts->port[i] = atoi(temp);
	}
}

void ngx_http_as_utils_get_parsed_url_arguement(ngx_str_t url, char* arg, char value[])
{
	char temp2[1000],temp_args[100][1000];
	int pos = 0, i;
	bool flag = false;
	char *removespace = strtok((char*)url.data, " ");  //contains get parameters without space
	char removed_space[url.len+1];
	strcpy(removed_space, removespace);

	while(removespace!=NULL)
	{
		removespace= strtok(NULL, " ");
	}
	char *temp = strtok(removed_space, "&");
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
			strcpy(value, temp);
			replace(value,"%22","");
			break;
		}
	}
	if(flag)
	{
		replace(value,"%22","");
	}
	
	
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

void replace(char * o_string, char * s_string, char * r_string) {
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
      return replace(o_string, s_string, r_string);
 }

 int ngx_http_as_utils_put(ngx_http_request_t *r, ngx_str_t url, aerospike *as)
{
	//ngx_http_as_conf_t *as_conf = ngx_http_get_module_loc_conf(r, ngx_http_as_module);

	char key[1000], namespace[40], set[100], bin[1000], value[1000];

	ngx_http_as_utils_get_parsed_url_arguement(url, "ns", namespace);
	ngx_http_as_utils_get_parsed_url_arguement(url, "set", set);
	ngx_http_as_utils_get_parsed_url_arguement(url, "key", key);
	ngx_http_as_utils_get_parsed_url_arguement(url, "bin", bin);
	ngx_http_as_utils_get_parsed_url_arguement(url, "value", value);

	as_key put_key;
	as_key_init(&put_key, namespace, set, key);

	as_record rec;
	as_record_inita(&rec, 1);
	as_record_set_str(&rec, bin, value);

	as_error err;
	if(aerospike_key_put(as, &err, NULL, &put_key, &rec)!=AEROSPIKE_OK)
	{
		return 0;
	}
	else
		return 1;
}