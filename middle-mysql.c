/* Implements the mid-layer processing for osm2pgsql
 * using several MySQL tables
 * 
 * This layer stores data read in from the planet.osm file
 * and is then read by the backend processing code to
 * emit the final geometry-enabled output formats
*/

#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include <mysql/mysql.h>
#include "mysql.h"

#include "osmtypes.h"

#include "middle.h"
#include "middle-mysql.h"
#include "output.h"

#include "node-ram-cache.h"

static MYSQL sql_conn;
static const struct output_options *my_options;

static my_drop_tables(void)
{
  mysql_vexec(&sql_conn, "DROP TABLE IF EXISTS %s_nodes", my_options->prefix);
  mysql_vexec(&sql_conn, "DROP TABLE IF EXISTS %s_ways", my_options->prefix);
  mysql_vexec(&sql_conn, "DROP TABLE IF EXISTS %s_way_nodes", my_options->prefix);
  mysql_vexec(&sql_conn, "DROP TABLE IF EXISTS %s_rels", my_options->prefix);
  mysql_vexec(&sql_conn, "DROP TABLE IF EXISTS %s_rel_parts", my_options->prefix);
}

static int my_start(const struct output_options *options)
{
  my_options = options;

  fprintf(stderr, "foo %p %p\n", options, my_options);

  /* connect to mysql or exit_nicely() */
  if (NULL == mysql_real_connect(&sql_conn, options->conn.host, options->conn.username, options->conn.password, options->conn.db, 3306 /* FIXME */, NULL, 0)) {
    fprintf(stderr, "mysql middle connect failed: host %s:, db %s, user: %s, pwd: %s\n", options->conn.host, options->conn.db, options->conn.username, options->conn.password);
    exit_nicely();
  } else {
    fprintf(stderr, "mysql middle connect succeeded\n");
  }
  
  /* set up local ram caches */
  init_node_ram_cache( options->alloc_chunkwise | ALLOC_LOSSY, options->cache, options->scale);
  if (options->flat_node_cache_enabled) init_node_persistent_cache(options, options->append);
    
  if (!options->append) { 
    /* drop and (re-)create tables */
    my_drop_tables();

    mysql_vexec(&sql_conn, "CREATE TABLE %s_nodes(id bigint not null primary key, lat int not null, lon int not null, tags text) ENGINE=myisam", options->prefix);

    mysql_vexec(&sql_conn, "CREATE TABLE %s_ways(id bigint not null primary key, tags text, pending bool) ENGINE=myisam", options->prefix);

    mysql_vexec(&sql_conn, "CREATE TABLE %s_way_nodes(way_id bigint not null, node_id bigint not null, sequence int not null, primary key(way_id, node_id, sequence)) ENGINE=myisam", options->prefix);

    mysql_vexec(&sql_conn, "CREATE TABLE %s_rels(id bigint not null primary key, tags text, pending bool) ENGINE=myisam", options->prefix);

    mysql_vexec(&sql_conn, "CREATE TABLE %s_rel_parts(rel_id bigint not null, part_id bigint not null, part_type char not null, role varchar(100), sequence int not null, primary key(rel_id, part_id, part_type, sequence)) ENGINE=myisam", options->prefix);

    /* FIXME: ALTER TABLE ... DISABLE KEYS here? */
  }

  /* diagnostics */
  fprintf(stderr, "Mid: mysql, scale=%d cache=%d\n", options->scale, options->cache);

  return 0;
}

static void my_stop(void) 
{
  /* shut down local ram caches */
  free_node_ram_cache();
  if (my_options->flat_node_cache_enabled) shutdown_node_persistent_cache();

  /* FIXME: either enable keys and maybe analyze/optimize table */  

  /* or drop table */
  my_drop_tables();

  /* close mysql connection */
  mysql_close(&sql_conn);
}

static void my_cleanup(void)
{
  /* unused */
}

static void my_analyze(void)
{
  /* unused */
}

static void my_end(void) 
{
  /* unused */
}

static void my_commit(void) 
{
  /* just commit, simple as that here */
  mysql_exec(&sql_conn, "COMMIT");
}

/* Special escape routine for escaping strings in array constants: double quote, backslash,newline, tab*/
static char *escape_tag( char *ptr, const char *in, int escape )
{
  while( *in )
  {
    switch(*in)
    {
      case '"':
        if( escape ) *ptr++ = '\\';
        *ptr++ = '\\';
        *ptr++ = '"';
        break;
      case '\'':
        if( escape ) *ptr++ = '\\';
        *ptr++ = '\\';
        *ptr++ = '\'';
        break;
      case '\\':
        if( escape ) *ptr++ = '\\';
        if( escape ) *ptr++ = '\\';
        *ptr++ = '\\';
        *ptr++ = '\\';
        break;
      case '\n':
        if( escape ) *ptr++ = '\\';
        *ptr++ = '\\';
        *ptr++ = 'n';
        break;
      case '\r':
        if( escape ) *ptr++ = '\\';
        *ptr++ = '\\';
        *ptr++ = 'r';
        break;
      case '\t':
        if( escape ) *ptr++ = '\\';
        *ptr++ = '\\';
        *ptr++ = 't';
        break;
      default:
        *ptr++ = *in;
        break;
    }
    in++;
  }
  return ptr;
}




/* escape means we return '\N' for copy mode, otherwise we return just NULL */
static char *mysql_store_tags(struct keyval *tags, int escape)
{
  static char *buffer;
  static int buflen;

  char *ptr;
  struct keyval *i;
  int first;
    
  int countlist = countList(tags);
  if( countlist == 0 )
  {
    if( escape )
      return "\\N";
    else
      return NULL;
  }
    
  if( buflen <= countlist * 24 ) /* LE so 0 always matches */
  {
    buflen = ((countlist * 24) | 4095) + 1;  /* Round up to next page */
    buffer = realloc( buffer, buflen );
  }
_restart:

  ptr = buffer;
  first = 1;
  *ptr++ = '{';
  /* The lists are circular, exit when we reach the head again */
  for( i=tags->next; i->key; i = i->next )
  {
    int maxlen = (strlen(i->key) + strlen(i->value)) * 4;
    if( (ptr+maxlen-buffer) > (buflen-20) ) /* Almost overflowed? */
    {
      buflen <<= 1;
      buffer = realloc( buffer, buflen );
      
      goto _restart;
    }
    if( !first ) *ptr++ = ',';
    *ptr++ = '"';
    ptr = escape_tag( ptr, i->key, escape );
    *ptr++ = '"';
    *ptr++ = ',';
    *ptr++ = '"';
    ptr = escape_tag( ptr, i->value, escape );
    *ptr++ = '"';
    
    first=0;
  }
  
  *ptr++ = '}';
  *ptr++ = 0;
  
  return buffer;
}


/* Decodes a portion of an array literal from postgres */
/* Argument should point to beginning of literal, on return points to delimiter */
static const char *decode_upto( const char *src, char *dst )
{
  int quoted = (*src == '"');
  if( quoted ) src++;
  
  while( quoted ? (*src != '"') : (*src != ',' && *src != '}') )
  {
    if( *src == '\\' )
    {
      switch( src[1] )
      {
        case 'n': *dst++ = '\n'; break;
        case 't': *dst++ = '\t'; break;
        default: *dst++ = src[1]; break;
      }
      src+=2;
    }
    else
      *dst++ = *src++;
  }
  if( quoted ) src++;
  *dst = 0;
  return src;
}

static void mysql_parse_tags( const char *string, struct keyval *tags )
{
  char key[1024];
  char val[1024];
  
  if( *string == '\0' )
    return;
    
  if( *string++ != '{' )
    return;
  while( *string != '}' )
  {
    string = decode_upto( string, key );
    /* String points to the comma */
    string++;
    string = decode_upto( string, val );
    /* String points to the comma or closing '}' */
    addItem( tags, key, val, 0 );
    if( *string == ',' )
      string++;
  }
}

static int my_nodes_set(osmid_t id, double lat, double lon, struct keyval *tags) 
{
  mysql_vexec(&sql_conn, "INSERT INTO %s_nodes SET id = %" PRIdOSMID ", lat = %.0f, lon = %.0f, tags = '%s'", my_options->prefix, id, lat, lon, mysql_store_tags(tags, 0));

  return 0;
}

static int my_nodes_get_list(struct osmNode *nodes, osmid_t *node_ids, int node_count)
{
  int n, i;
  char *query, *p;
  long count = 0, count_db = 0, count_query = 0;
  MYSQL_RES *result;
  osmid_t *query_nodeids, *p_nodeid;
  struct osmNode *query_nodes, *p_node;
  MYSQL_ROW row;

  query = (char *)malloc(100 + node_count * 15); // big enough for sure
  n = sprintf(query, "SELECT id, lat, lon FROM %s_nodes WHERE id IN (", my_options->prefix);
  p = query + n;

  for (i = 0; i < node_count; i++) {
    /* Check cache first */ 
    if( ram_cache_nodes_get( &nodes[i], node_ids[i]) == 0 ) {
      count++;
      continue;
    }

    count_db++;

    /* Mark nodes as needing to be fetched from the DB */
    nodes[i].lat = NAN;
    nodes[i].lon = NAN;

    // add node id to query string
    p += sprintf(p, " %" PRIdOSMID ",", node_ids[i]);
  }

  // replace the last ',' in the query with ')'
  *p = '\0';
  --p;
  *p = ')';

  if ( 0 != mysql_query(&sql_conn, query)) {
    fprintf(stderr, "query error: %s\n", mysql_error(&sql_conn));
    exit_nicely();
  }

  result = mysql_store_result(&sql_conn);

  count_query = (long)mysql_num_rows(result);

  query_nodeids = p_nodeid = (osmid_t*)calloc(count_query, sizeof(osmid_t));
  query_nodes = p_node = (struct osmNode *)calloc(count_query, sizeof(struct osmNode));
  /* FIXME: error handling */
  
  while ((row = mysql_fetch_row(result))) {
  }

  mysql_free_result(result);
  
  return 0;
}

static int my_nodes_delete(osmid_t id) 
{
  return 0;
}

static int my_node_changed(osmid_t id) 
{
  return 0;
}

/* static int my_nodes_get(struct osmNode *out, osmid_t id); */

static int my_ways_set(osmid_t id, osmid_t *nds, int nd_count, struct keyval *tags, int pending) 
{
  return 0;
}

static int my_ways_get(osmid_t id, struct keyval *tag_ptr, struct osmNode **node_ptr, int *count_ptr) 
{
  return 0;
}

static int my_ways_get_list(osmid_t *ids, int way_count, osmid_t **way_ids, struct keyval *tag_ptr, struct osmNode **node_ptr, int *count_ptr) 
{
  return 0;
}

static int my_ways_done(osmid_t id) 
{
  return 0;
}

static int my_ways_delete(osmid_t id) 
{
  return 0;
}

static int my_way_changed(osmid_t id) 
{
  return 0;
}

static int my_relations_set(osmid_t id, struct member *members, int member_count, struct keyval *tags) 
{
  return 0;
}

/* static int my_relations_get(osmid_t id, struct member **members, int *member_count, struct keyval *tags); */

static int my_relations_done(osmid_t id) 
{
  return 0;
}

static int my_relations_delete(osmid_t id) 
{
  return 0;
}

static int my_relations_changed(osmid_t id) 
{
  return 0;
}

/* static void my_iterate_nodes(int callback)(osmid_t id, struct keyval *tags, double node_lat, double node_lon)); */

static void my_iterate_ways(int (*callback)(osmid_t id, struct keyval *tags, struct osmNode *nodes, int count, int exists)) 
{
}

static void my_iterate_relations(int (*callback)(osmid_t id, struct member *, int member_count, struct keyval *rel_tags, int exists)) 
{
}

struct middle_t mid_mysql = {
        .start             = my_start,
        .stop              = my_stop,
        .cleanup           = my_cleanup,
        .analyze           = my_analyze,
        .end               = my_end,
        .commit            = my_commit,

        .nodes_set         = my_nodes_set,
#if 0
        .nodes_get         = my_nodes_get,
#endif
        .nodes_get_list    = my_nodes_get_list,
        .nodes_delete	   = my_nodes_delete,
        .node_changed      = my_node_changed,

        .ways_set          = my_ways_set,
        .ways_get          = my_ways_get,
        .ways_get_list     = my_ways_get_list,
        .ways_done         = my_ways_done,
        .ways_delete       = my_ways_delete,
        .way_changed       = my_way_changed,

        .relations_set     = my_relations_set,
#if 0
        .relations_get     = my_relations_get,
#endif
        .relations_done    = my_relations_done,
        .relations_delete  = my_relations_delete,
        .relation_changed  = my_relations_changed,
#if 0
        .iterate_nodes     = my_iterate_nodes,
#endif
        .iterate_ways      = my_iterate_ways,
        .iterate_relations = my_iterate_relations
};
