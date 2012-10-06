#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <errno.h>
#include <mysql/mysql.h>

#include "osmtypes.h"
#include "output.h"
#include "style-file.h"
#include "output-helper.h"
#include "output-mysql.h"

#define UNUSED  __attribute__ ((unused))

#if 0
#define DEBUG_MYSQL
#endif

static const struct output_options *Options;

/* Tables to output */
static struct s_table {
  //enum table_id table;
  char *name;
  const char *type;
  MYSQL *sql_conn;
  char buffer[1024];
  unsigned int buflen;
  char *columns;
} tables [] = {
  { .name = "%s_point",   .type = "POINT"     },
  { .name = "%s_line",    .type = "LINESTRING"},
  { .name = "%s_polygon", .type = "GEOMETRY"  }, // Actually POLGYON & MULTIPOLYGON but no way to limit to just these two
  { .name = "%s_roads",   .type = "LINESTRING"}
};
#define NUM_TABLES ((signed)(sizeof(tables) / sizeof(tables[0])))

static int mysql_exec(MYSQL *sql_conn, const char *fmt, ...);

/* Escape data appropriate to the type */
static void escape_type(char *sql, int len, const char *value, const char *type) {
  int items;
  static int tmplen=0;
  static char *tmpstr;

  if (len > tmplen) {
    tmpstr=realloc(tmpstr,len);
    tmplen=len;
  }
  strcpy(tmpstr,value);

  if ( !strcmp(type, "int4") ) {
    int from, to; 
    /* For integers we take the first number, or the average if it's a-b */
    items = sscanf(value, "%d-%d", &from, &to);
    if ( items == 1 ) {
      sprintf(sql, "%d", from);
    } else if ( items == 2 ) {
      sprintf(sql, "%d", (from + to) / 2);
    } else {
      sprintf(sql, "NULL");
    }
  } else {
    /*
    try to "repair" real values as follows:
      * assume "," to be a decimal mark which need to be replaced by "."
      * like int4 take the first number, or the average if it's a-b
      * assume SI unit (meters)
      * convert feet to meters (1 foot = 0.3048 meters)
      * reject anything else    
    */
    if ( !strcmp(type, "real") ) {
      int i,slen;
      float from,to;

      slen=strlen(value);
      for (i=0;i<slen;i++) if (tmpstr[i]==',') tmpstr[i]='.';

      items = sscanf(tmpstr, "%f-%f", &from, &to);
      if ( items == 1 ) {
	if ((tmpstr[slen-2]=='f') && (tmpstr[slen-1]=='t')) {
	  from*=0.3048;
	}
	sprintf(sql, "%f", from);
      } else if ( items == 2 ) {
	if ((tmpstr[slen-2]=='f') && (tmpstr[slen-1]=='t')) {
	  from*=0.3048;
	  to*=0.3048;
	}
	sprintf(sql, "%f", (from + to) / 2);
      } else {
	sprintf(sql, "NULL");
      }
    } else {
      *sql = '\'';
      mysql_escape_string(sql+1, value, strlen(value));
      strcat(sql, "'");
    }
  }
}

static int mysql_out_node(osmid_t id, struct keyval *tags, double node_lat, double node_lon)
{
    static char *sql;
    static size_t sqllen=0;
    char *v;
    int i;
    struct keyval *tag;
    char buffer[1024];

    if (sqllen==0) {
      sqllen=2048;
      sql=malloc(sqllen);
    }

    /* FIXME expire_tiles_from_bbox(node_lon, node_lat, node_lon, node_lat); */
    sprintf(sql, "INSERT INTO %s (%s,`way`) VALUES (%ld", tables[t_point].name, tables[t_point].columns, id);

    for (i=0; i < exportListCount[OSMTYPE_NODE]; i++) {
        if( exportList[OSMTYPE_NODE][i].flags & FLAG_DELETE )
            continue;
        if( (exportList[OSMTYPE_NODE][i].flags & FLAG_PHSTORE) == FLAG_PHSTORE)
            continue;
        if ((tag = getTag(tags, exportList[OSMTYPE_NODE][i].name)))
        {
	  escape_type(buffer, sizeof(buffer), tag->value, exportList[OSMTYPE_NODE][i].type);
	  strcat(sql, ",");
	  strcat(sql, buffer);
	  exportList[OSMTYPE_NODE][i].count++;
	  /* FIXME 
	  if (HSTORE_NORM==Options->enable_hstore)
	    tag->has_column=1;
	  */
        }
        else
	  strcat(sql, ",NULL");
    }
    sprintf(buffer, ",GeomFromText('POINT(%.15g %.15g)'))", node_lon, node_lat);
    strcat(sql, buffer);

    mysql_exec(tables[t_point].sql_conn, sql);

    /* FIXME: hstore */

    return 0;
}

static int mysql_out_way(osmid_t id, struct keyval *tags, struct osmNode *nodes, int count, int exists)
{
  /* FIXME */
  exit(3);
}

static int mysql_out_relation(osmid_t id, struct keyval *rel_tags, struct osmNode **xnodes, struct keyval *xtags, int *xcount, osmid_t *xid, const char **xrole)
{
  /* FIXME */
  exit(3);
}

static int mysql_delete_way_from_output(osmid_t osm_id)
{
  /* FIXME */
  exit(3);
}

static int mysql_delete_relation_from_output(osmid_t osm_id)
{
  /* FIXME */
  exit(3);
}


static int mysql_process_relation(osmid_t id, struct member *members, int member_count, struct keyval *tags, int exists)
{
  /* FIXME */
  exit(3);
}

static void mysql_out_commit(void) {
  int i;
  for (i=0; i<NUM_TABLES; i++) {
    fprintf(stderr, "Committing transaction for %s\n", tables[i].name);
    mysql_exec(tables[i].sql_conn, "COMMIT");
  }
}

static void mysql_out_cleanup(void) 
{
  int i;
  
  for (i=0; i<NUM_TABLES; i++) {
    if (tables[i].sql_conn) {
      mysql_close(tables[i].sql_conn);
      tables[i].sql_conn = NULL;
    }
  }
}

static int mysql_out_start(const struct output_options *options) 
{
  char *sql, tmp[256];
  unsigned int sql_len;
  int i, j;
  Options = options;

  read_style_file( options->style, options);

  sql_len = 2048;
  sql = malloc(sql_len);
  assert(sql);

  for (i=0; i<NUM_TABLES; i++) {
    MYSQL *sql_conn = mysql_init(NULL);
    
    if (NULL == mysql_real_connect(sql_conn, options->conn.host, options->conn.username, options->conn.password, options->conn.db, 3306 /* FIXME */, NULL, 0)) {
      fprintf(stderr, "mysql connect failed: host %s:, db %s, user: %s, pwd: %s\n", options->conn.host, options->conn.db, options->conn.username, options->conn.password);
    } else {
      fprintf(stderr, "mysql connect succeeded\n");
    }
    tables[i].sql_conn = sql_conn;

    /* Substitute prefix into name of table */
    {
      char *temp = malloc( strlen(options->prefix) + strlen(tables[i].name) + 1 );
      sprintf( temp, tables[i].name, options->prefix );
      tables[i].name = temp;
    }

    mysql_exec(sql_conn, "SET NAMES utf8");

    /* FIXME: needs SUPER privs 
       mysql_exec(sql_conn, "set global innodb_flush_log_at_trx_commit=0";
    */
    
    fprintf(stderr, "Setting up table: %s\n", tables[i].name);
    
    if (!options->append) {
      mysql_exec(sql_conn, "DROP TABLE IF EXISTS %s", tables[i].name);
      mysql_exec(sql_conn, "DROP TABLE IF EXISTS %s_tmp", tables[i].name);
    } else { /* append */
      fprintf(stderr, "output-mysql doesn't support --append yet\n");
      exit_nicely();      
    }
    
    enum OsmType type = (i == t_point) ? OSMTYPE_NODE : OSMTYPE_WAY;
    int numTags = exportListCount[type];
    struct taginfo *exportTags = exportList[type];
    
    if (!options->append) {
      sprintf(sql, "CREATE TABLE %s ( osm_id BIGINT", tables[i].name );
      for (j=0; j < numTags; j++) {
	if( exportTags[j].flags & FLAG_DELETE )
	  continue;
	if( (exportTags[j].flags & FLAG_PHSTORE ) == FLAG_PHSTORE)
	  continue;
	sprintf(tmp, ",`%s` %s", exportTags[j].name, exportTags[j].type);
	if (strlen(sql) + strlen(tmp) + 1 > sql_len) {
	  sql_len *= 2;
	  sql = realloc(sql, sql_len);
	  assert(sql);
	}
	strcat(sql, tmp);
      }
      /* FIXME: ignoring hstore completely for now, not even throwing errors */
      /* FIXME: same for tablespace specs */
      /* FIXME: ignoring --slim specific stuff, too */
      
      strcat(sql, ", way ");
      strcat(sql, tables[i].type);
      strcat(sql, " NOT NULL, SPATIAL INDEX(way) ");
      strcat(sql, " ) ENGINE=MyISAM");
      
#ifdef DEBUG_MYSQL      
      fprintf(stderr, "SQL: %s\n", sql);
#endif
      mysql_exec(sql_conn, sql);
    } else { /* append */
      fprintf(stderr, "output-mysql doesn't support --append yet\n");
      exit_nicely(); 
    }

    /* Generate column list for INSERT */
    strcpy(sql, "`osm_id`");
    for (j=0; j < numTags; j++) {
      if( exportTags[j].flags & FLAG_DELETE )
	continue;
      if( (exportTags[j].flags & FLAG_PHSTORE ) == FLAG_PHSTORE)
	continue;
      sprintf(tmp, ",`%s`", exportTags[j].name);
      
      if (strlen(sql) + strlen(tmp) + 1 > sql_len) {
	sql_len *= 2;
	sql = realloc(sql, sql_len);
	assert(sql);
      }
      strcat(sql, tmp);
    }

    /* FIXME hstore */
    
    tables[i].columns = strdup(sql);


    mysql_exec(sql_conn, "BEGIN");
  }  
    
  /* FIXME prepare get_wkt? */

  /* FIXME how to deal with COPY mode replacement? */

  free(sql);

  /* FIXME expire_tiles_init(options); */
  
  options->mid->start(options);

  return 0;
}

static void mysql_out_stop() {
  int i;

  mysql_out_commit();
  Options->mid->commit();

  for (i=0; i<NUM_TABLES; i++) {
    MYSQL *sql_conn = tables[i].sql_conn;
    mysql_exec(sql_conn, "BEGIN");
  }

  /* Processing any remaing to be processed ways */
  Options->mid->iterate_ways(mysql_out_way );
  mysql_out_commit();
  Options->mid->commit();

  /* Processing any remaing to be processed relations */
  /* During this stage output tables also need to stay out of
   * extended transactions, as the delete_way_from_output, called
   * from process_relation, can deadlock if using multi-processing.
   */    
  Options->mid->iterate_relations( mysql_process_relation );

  /* No longer need to access middle layer -- release memory */
  Options->mid->stop();
  for (i=0; i<NUM_TABLES; i++) {
    /* TODO: OPTIMIZE/ANALYZE TABLE here? */
    mysql_close(tables[i].sql_conn);
    free(tables[i].name);
    free(tables[i].columns);
  }

  mysql_out_cleanup();
  free_style();
  
  /* FIXME expire_tiles_stop(); */
}

static int mysql_out_connect(const struct output_options *options, int startTransaction) {
  return 0;
}

static void mysql_out_close(int stopTransaction) {
  int i;
  for (i=0; i<NUM_TABLES; i++) {
    if (stopTransaction)
      mysql_exec(tables[i].sql_conn, "COMMIT");
    mysql_close(tables[i].sql_conn);
    tables[i].sql_conn = NULL;
  }
}

static int mysql_add_node(osmid_t id, double lat, double lon, struct keyval *tags) 
{
  int polygon;
  int filter = filter_tags(OSMTYPE_NODE, tags, &polygon, Options);
  
  Options->mid->nodes_set(id, lat, lon, tags);
  if( !filter )
    mysql_out_node(id, tags, lat, lon);

  return 0;
}


static int mysql_add_way(osmid_t id, osmid_t *nds, int nd_count, struct keyval *tags)
{
  int polygon = 0;

  // Check whether the way is: (1) Exportable, (2) Maybe a polygon
  int filter = filter_tags(OSMTYPE_WAY, tags, &polygon, Options);

  // If this isn't a polygon then it can not be part of a multipolygon
  // Hence only polygons are "pending"
  Options->mid->ways_set(id, nds, nd_count, tags, (!filter && polygon) ? 1 : 0);

  if( !polygon && !filter )
  {
    /* Get actual node data and generate output */
    struct osmNode *nodes = malloc( sizeof(struct osmNode) * nd_count );
    int count = Options->mid->nodes_get_list( nodes, nds, nd_count );
    mysql_out_way(id, tags, nodes, count, 0);
    free(nodes);
  }
  return 0;
}

static int mysql_add_relation(osmid_t id, struct member *members, int member_count, struct keyval *tags)
{
  const char *type = getItem(tags, "type");

  // Must have a type field or we ignore it
  if (!type)
      return 0;

  /* In slim mode we remember these*/
  if(Options->mid->relations_set)
    Options->mid->relations_set(id, members, member_count, tags);
  // (osmid_t id, struct keyval *rel_tags, struct osmNode **xnodes, struct keyval **xtags, int *xcount)

  return mysql_process_relation(id, members, member_count, tags, 0);
}

static int mysql_delete_node(osmid_t osm_id)
{
  if( !Options->slim ) {
    fprintf( stderr, "Cannot apply diffs unless in slim mode\n" );
    exit_nicely();
  }

  /* FIXME
     if ( expire_tiles_from_db(tables[t_point].sql_conn, osm_id) != 0)
  */
  mysql_exec(tables[t_point].sql_conn, "DELETE FROM %s WHERE osm_id = %", tables[t_point].name, osm_id );
  
  Options->mid->nodes_delete(osm_id);
  return 0;
}

static int mysql_delete_way(osmid_t osm_id)
{
  if( !Options->slim ) {
    fprintf( stderr, "Cannot apply diffs unless in slim mode\n" );
    exit_nicely();
  }
  mysql_delete_way_from_output(osm_id);
  Options->mid->ways_delete(osm_id);
  return 0;
}

static int mysql_delete_relation(osmid_t osm_id)
{
  if( !Options->slim ) {
    fprintf( stderr, "Cannot apply diffs unless in slim mode\n" );
    exit_nicely();
  }
  mysql_delete_relation_from_output(osm_id);
  Options->mid->relations_delete(osm_id);
  return 0;
}


static int mysql_modify_node(osmid_t osm_id, double lat, double lon, struct keyval *tags)
{
  if( !Options->slim ) {
    fprintf( stderr, "Cannot apply diffs unless in slim mode\n" );
    exit_nicely();
  }
  mysql_delete_node(osm_id);
  mysql_add_node(osm_id, lat, lon, tags);
  Options->mid->node_changed(osm_id);
  return 0;
}

static int mysql_modify_way(osmid_t osm_id, osmid_t *nodes, int node_count, struct keyval *tags)
{
    if( !Options->slim )
    {
        fprintf( stderr, "Cannot apply diffs unless in slim mode\n" );
        exit_nicely();
    }
    mysql_delete_way(osm_id);
    mysql_add_way(osm_id, nodes, node_count, tags);
    Options->mid->way_changed(osm_id);
    return 0;
}

static int mysql_modify_relation(osmid_t osm_id, struct member *members, int member_count, struct keyval *tags)
{
  if( !Options->slim ) {
    fprintf( stderr, "Cannot apply diffs unless in slim mode\n" );
    exit_nicely();
  }
  mysql_delete_relation(osm_id);
  mysql_add_relation(osm_id, members, member_count, tags);
  Options->mid->relation_changed(osm_id);
  return 0;
}

static int mysql_exec(MYSQL *sql_conn, const char *fmt, ...)
{
  va_list ap;
  char *sql, *nsql;
  int n, res, size = 1000;
  MYSQL_RES *result;

  if ((sql = malloc(size)) == NULL) {
    fprintf(stderr, "Memory allocation failed\n");
    exit_nicely();
  }

  while (1) {
    /* Try to print in the allocated space. */
    va_start(ap, fmt);
    n = vsnprintf(sql, size, fmt, ap);
    va_end(ap);
    /* If that worked, return the string. */
    if (n > -1 && n < size)
      break;
    /* Else try again with more space. */
    if (n > -1)    /* glibc 2.1 */
      size = n+1; /* precisely what is needed */
    else           /* glibc 2.0 */
      size *= 2;  /* twice the old size */
    if ((nsql = realloc (sql, size)) == NULL) {
      free(sql);
      fprintf(stderr, "Memory re-allocation failed\n");
      exit_nicely();
    } else {
      sql = nsql;
    }
  }

#ifdef DEBUG_MYSQL
  fprintf( stderr, "Executing: %s\n", sql );
#endif
  res = mysql_query(sql_conn, sql);
  if (res) {
    fprintf(stderr, "%s failed: %s\n", sql, mysql_error(sql_conn));
    free(sql);
    exit_nicely();
  }
  free(sql);

  result = mysql_store_result(sql_conn);  
  if (result) {
    mysql_free_result(result);
  }

  return 0;
}

struct output_t out_mysql = {
 .start           = mysql_out_start,
 .stop            = mysql_out_stop,
 .connect         = mysql_out_connect,
 .close           = mysql_out_close,
 .cleanup         = mysql_out_cleanup,

 .node_add        = mysql_add_node,
 .way_add         = mysql_add_way,
 .relation_add    = mysql_add_relation,
 
 .node_modify     = mysql_modify_node,
 .way_modify      = mysql_modify_way,
 .relation_modify = mysql_modify_relation,
 
 .node_delete     = mysql_delete_node,
 .way_delete      = mysql_delete_way,
 .relation_delete = mysql_delete_relation
};
