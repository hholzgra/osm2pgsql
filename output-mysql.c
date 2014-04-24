#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <mysql/mysql.h>

#include "osmtypes.h"
#include "reprojection.h"
#include "output.h"
#include "output-helper.h"
#include "output-mysql.h"
#include "style-file.h"
#include "build_geometry.h"

#include "mysql.h"

/* FIXME */
#define PGconn void
#include "expire-tiles.h"

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
  char **column_names;
} tables [] = {
  { .name = "%s_point",   .type = "POINT"     },
  { .name = "%s_line",    .type = "LINESTRING"},
  { .name = "%s_polygon", .type = "GEOMETRY"  }, // Actually POLGYON & MULTIPOLYGON but no way to limit to just these two
  { .name = "%s_roads",   .type = "LINESTRING"}
};
#define NUM_TABLES ((signed)(sizeof(tables) / sizeof(tables[0])))

/* Escape data appropriate to the type */
static int escape_type(char *sql, int len, const char *value, const char *type) {
  int items;
  static int tmplen=0;
  static char *tmpstr=NULL;

  if ( !strcmp(type, "int4") ) {
    int from, to; 
    /* For integers we take the first number, or the average if it's a-b */
    items = sscanf(value, "%d-%d", &from, &to);
    if ( items == 1 ) {
      return sprintf(sql, "%d", from);
    } else if ( items == 2 ) {
      return sprintf(sql, "%d", (from + to) / 2);
    } else {
      strcpy(sql, "NULL");
      return 4;
    }
  } else {
    if (len > tmplen) {
      tmpstr=realloc(tmpstr,len);
      tmplen=len;
    }
    strcpy(tmpstr,value);
    
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
	return sprintf(sql, "%f", from);
      } else if ( items == 2 ) {
	if ((tmpstr[slen-2]=='f') && (tmpstr[slen-1]=='t')) {
	  from*=0.3048;
	  to*=0.3048;
	}
	return sprintf(sql, "%f", (from + to) / 2);
      } else {
	strcpy(sql, "NULL");
	return 4;
      }
    } else {
      int tmp_len;

      *sql++ = '\''; 
      tmp_len = mysql_escape_string(sql, value, strlen(value));
      sql += tmp_len;
      *sql++ = '\'';

      return tmp_len + 2;
    }
  }

  return 0;
}

static void write_wkts(osmid_t id, struct keyval *tags, const char *wkt, enum table_id table)
{
    static char *sql, *p;
    static size_t sqllen=0, needed;
    int i, j, len, tmp_len;
    struct keyval *tag;

    // MySQL doesn't allow NULL values in spatial indexes
    // TODO: do we have to deal with MULTI* geometries where only some 
    //       of the elements are EMPTY?
    if (strstr(wkt, "EMPTY")) {
      return;
    }

    if (sqllen==0) {
      sqllen=4096;
      sql=malloc(sqllen);
    }
    
    len = sprintf(sql, "INSERT DELAYED INTO %s (osm_id", tables[table].name);
    p = sql + len;

    for (i=0; i < exportListCount[OSMTYPE_WAY]; i++) {
        if( exportList[OSMTYPE_WAY][i].flags & FLAG_DELETE )
            continue;
        if( (exportList[OSMTYPE_WAY][i].flags & FLAG_PHSTORE) == FLAG_PHSTORE)
            continue;
        if ((tag = getTag(tags, exportList[OSMTYPE_WAY][i].name)))
	{
	  *p++ = ',';
	  *p++ = '`';
	  strcpy(p, tables[table].column_names[i]);
	  tmp_len = strlen(tables[table].column_names[i]);
	  len += tmp_len + 3;
	  p+= tmp_len;
	  *p++ = '`';
	}
    }

    tmp_len = sprintf(p, ",way) VALUES (%" PRIdOSMID, id);
    p+= tmp_len;
    len += tmp_len;

    for (j=0; j < exportListCount[OSMTYPE_WAY]; j++) {
      if( exportList[OSMTYPE_WAY][j].flags & FLAG_DELETE )
	continue;
      if( (exportList[OSMTYPE_WAY][j].flags & FLAG_PHSTORE) == FLAG_PHSTORE)
	continue;
      if ((tag = getTag(tags, exportList[OSMTYPE_WAY][j].name)))
	{
	  exportList[OSMTYPE_WAY][j].count++;
	  *p++ = ','; len++;
	  tmp_len = escape_type(p, sqllen - len, tag->value, exportList[OSMTYPE_WAY][j].type);
	  p+= tmp_len;
	  len += tmp_len;
	  /* FIXME
	     if (HSTORE_NORM==Options->enable_hstore)
	     tag->has_column=1;
	  */
	}
    }
    
    /* FIXME
    // hstore columns
    write_hstore_columns(table, tags);
    
    // check if a regular hstore is requested
    if (Options->enable_hstore)
    write_hstore(table, tags);
    */    

    strcpy(p, ",GeomFromText('");
    p += 15;
    len += 15;
    tmp_len = strlen(wkt);
    needed = len + tmp_len +10; 
    if (needed > sqllen) {
      sqllen = needed;
      sql = realloc(sql, sqllen);
      if (!sql) {
	fprintf(stderr, "realloc %ld failed - out of memory?\n", sqllen);
	exit_nicely();
      }
      p = sql + len;
    }
    
    strcpy(p, wkt);
    len += tmp_len;
    p += tmp_len;
    strcpy(p, "'))\0");

    mysql_exec(tables[table].sql_conn, sql);
}

static int mysql_out_node(osmid_t id, struct keyval *tags, double node_lat, double node_lon)
{
    static char *sql, *p;
    static size_t sqllen=0;
    int i, len, tmp_len;
    struct keyval *tag;
    char buffer[1024];

    if (sqllen==0) {
      sqllen=2048;
      sql=malloc(sqllen);
    }

    expire_tiles_from_bbox(node_lon, node_lat, node_lon, node_lat); 

    len = sprintf(sql, "INSERT DELAYED INTO %s (osm_id", tables[t_point].name);
    p = sql + len;

    for (i=0; i < exportListCount[OSMTYPE_NODE]; i++) {
        if( exportList[OSMTYPE_NODE][i].flags & FLAG_DELETE )
            continue;
        if( (exportList[OSMTYPE_NODE][i].flags & FLAG_PHSTORE) == FLAG_PHSTORE)
            continue;
        if ((tag = getTag(tags, exportList[OSMTYPE_NODE][i].name)))
	{
	  *p++ = ',';
	  *p++ = '`';
	  strcpy(p, tables[t_point].column_names[i]);
	  p+= strlen(tables[t_point].column_names[i]);
	  *p++ = '`';
	}
    }

    tmp_len = sprintf(p, ",way) VALUES (%" PRIdOSMID, id);
    p+= tmp_len;

    for (i=0; i < exportListCount[OSMTYPE_NODE]; i++) {
        if( exportList[OSMTYPE_NODE][i].flags & FLAG_DELETE )
            continue;
        if( (exportList[OSMTYPE_NODE][i].flags & FLAG_PHSTORE) == FLAG_PHSTORE)
            continue;
        if ((tag = getTag(tags, exportList[OSMTYPE_NODE][i].name)))
	{
	  *p++ = ',';
	  tmp_len = escape_type(p, sqllen - len, tag->value, exportList[OSMTYPE_NODE][i].type);
	  p += tmp_len;
	  len += tmp_len + 1;

	  exportList[OSMTYPE_NODE][i].count++;
	  /* FIXME 
	  if (HSTORE_NORM==Options->enable_hstore)
	    tag->has_column=1;
	  */
        }
    }

    tmp_len = sprintf(p, ",GeomFromText('POINT(%.15g %.15g)'))", node_lon, node_lat);
    p += tmp_len;
    //    strcpy(p, "'))\0");

    mysql_exec(tables[t_point].sql_conn, sql);

    /* FIXME: hstore */

    return 0;
}


/* Seperated out because we use it elsewhere */
static int mysql_delete_way_from_output(osmid_t osm_id)
{
    /* Optimisation: we only need this is slim mode */
    if( !Options->slim )
        return 0;
    /* in droptemp mode we don't have indices and this takes ages. */
    if (Options->droptemp)
        return 0;
    mysql_vexec(tables[t_roads].sql_conn, "DELETE FROM %s WHERE osm_id = %"PRIdOSMID, tables[t_roads].name, osm_id );
    /* FIXME if ( expire_tiles_from_db(tables[t_line].sql_conn, osm_id) != 0) */
    mysql_vexec(tables[t_line].sql_conn, "DELETE FROM %s WHERE osm_id = %"PRIdOSMID, tables[t_line].name, osm_id );
    /* FIXME if ( expire_tiles_from_db(tables[t_poly].sql_conn, osm_id) != 0) */
    mysql_vexec(tables[t_poly].sql_conn, "DELETE FROM %s WHERE osm_id = %"PRIdOSMID, tables[t_poly].name, osm_id );
    return 0;
}

static int mysql_out_way(osmid_t id, struct keyval *tags, struct osmNode *nodes, int count, int exists)
{
  int polygon = 0, roads = 0;
  int i, wkt_size;
  double split_at;
  
  /* If the flag says this object may exist already, delete it first */
  if(exists) {
    mysql_delete_way_from_output(id);
    Options->mid->way_changed(id);
  }

  if (filter_tags(OSMTYPE_WAY, tags, &polygon, Options) || add_z_order(tags, &roads))
    return 0;

  // Split long ways after around 1 degree or 100km
  if (Options->projection == PROJ_LATLONG)
    split_at = 1;
  else
    split_at = 100 * 1000;
  
  wkt_size = get_wkt_split(nodes, count, polygon, split_at);
  
  for (i=0;i<wkt_size;i++) {
    char *wkt = get_wkt(i);
    
    if (wkt && strlen(wkt)) {
      /* FIXME: there should be a better way to detect polygons */
      if (!strncmp(wkt, "POLYGON", strlen("POLYGON")) || !strncmp(wkt, "MULTIPOLYGON", strlen("MULTIPOLYGON"))) {
	expire_tiles_from_nodes_poly(nodes, count, id);
	double area = get_area(i);
	if (area > 0.0) {
	  char tmp[32];
	  snprintf(tmp, sizeof(tmp), "%f", area);
	  addItem(tags, "way_area", tmp, 0);
	}
	write_wkts(id, tags, wkt, t_poly);
      } else {
	expire_tiles_from_nodes_line(nodes, count);
	write_wkts(id, tags, wkt, t_line);
	if (roads)
	  write_wkts(id, tags, wkt, t_roads);
      }
    }
    free(wkt);
  }
  clear_wkts();
  
  return 0;
}

static int mysql_out_relation(osmid_t id, struct keyval *rel_tags, struct osmNode **xnodes, struct keyval *xtags, int *xcount, osmid_t *xid, const char **xrole)
{
    int i, wkt_size;
    int polygon = 0, roads = 0;
    int make_polygon = 0;
    int make_boundary = 0;
    struct keyval tags, *p, poly_tags;
    char *type;
    double split_at;

#if 0
    fprintf(stderr, "Got relation with counts:");
    for (i=0; xcount[i]; i++)
        fprintf(stderr, " %d", xcount[i]);
    fprintf(stderr, "\n");
#endif
    /* Get the type, if there's no type we don't care */
    type = getItem(rel_tags, "type");
    if( !type )
        return 0;

    initList(&tags);
    initList(&poly_tags);

    /* Clone tags from relation */
    p = rel_tags->next;
    while (p != rel_tags) {
        // For routes, we convert name to route_name
        if ((strcmp(type, "route") == 0) && (strcmp(p->key, "name") ==0))
            addItem(&tags, "route_name", p->value, 1);
        else if (strcmp(p->key, "type")) // drop type=
            addItem(&tags, p->key, p->value, 1);
        p = p->next;
    }

    if( strcmp(type, "route") == 0 )
    {
        const char *state = getItem(rel_tags, "state");
        const char *netw = getItem(rel_tags, "network");
        int networknr = -1;

        if (state == NULL) {
            state = "";
        }

        if (netw != NULL) {
            if (strcmp(netw, "lcn") == 0) {
                networknr = 10;
                if (strcmp(state, "alternate") == 0) {
                    addItem(&tags, "lcn", "alternate", 1);
                } else if (strcmp(state, "connection") == 0) {
                    addItem(&tags, "lcn", "connection", 1);
                } else {
                    addItem(&tags, "lcn", "yes", 1);
                }
            } else if (strcmp(netw, "rcn") == 0) {
                networknr = 11;
                if (strcmp(state, "alternate") == 0) {
                    addItem(&tags, "rcn", "alternate", 1);
                } else if (strcmp(state, "connection") == 0) {
                    addItem(&tags, "rcn", "connection", 1);
                } else {
                    addItem(&tags, "rcn", "yes", 1);
                }
            } else if (strcmp(netw, "ncn") == 0) {
                networknr = 12;
                if (strcmp(state, "alternate") == 0) {
                    addItem(&tags, "ncn", "alternate", 1);
                } else if (strcmp(state, "connection") == 0) {
                    addItem(&tags, "ncn", "connection", 1);
                } else {
                    addItem(&tags, "ncn", "yes", 1);
                }


            } else if (strcmp(netw, "lwn") == 0) {
                networknr = 20;
                if (strcmp(state, "alternate") == 0) {
                    addItem(&tags, "lwn", "alternate", 1);
                } else if (strcmp(state, "connection") == 0) {
                    addItem(&tags, "lwn", "connection", 1);
                } else {
                    addItem(&tags, "lwn", "yes", 1);
                }
            } else if (strcmp(netw, "rwn") == 0) {
                networknr = 21;
                if (strcmp(state, "alternate") == 0) {
                    addItem(&tags, "rwn", "alternate", 1);
                } else if (strcmp(state, "connection") == 0) {
                    addItem(&tags, "rwn", "connection", 1);
                } else {
                    addItem(&tags, "rwn", "yes", 1);
                }
            } else if (strcmp(netw, "nwn") == 0) {
                networknr = 22;
                if (strcmp(state, "alternate") == 0) {
                    addItem(&tags, "nwn", "alternate", 1);
                } else if (strcmp(state, "connection") == 0) {
                    addItem(&tags, "nwn", "connection", 1);
                } else {
                    addItem(&tags, "nwn", "yes", 1);
                }
            }
        }

        if (getItem(rel_tags, "preferred_color") != NULL) {
            const char *a = getItem(rel_tags, "preferred_color");
            if (strcmp(a, "0") == 0 || strcmp(a, "1") == 0 || strcmp(a, "2") == 0 || strcmp(a, "3") == 0 || strcmp(a, "4") == 0) {
                addItem(&tags, "route_pref_color", a, 1);
            } else {
                addItem(&tags, "route_pref_color", "0", 1);
            }
        } else {
            addItem(&tags, "route_pref_color", "0", 1);
        }

        if (getItem(rel_tags, "ref") != NULL) {
            if (networknr == 10) {
                addItem(&tags, "lcn_ref", getItem(rel_tags, "ref"), 1);
            } else if (networknr == 11) {
                addItem(&tags, "rcn_ref", getItem(rel_tags, "ref"), 1);
            } else if (networknr == 12) {
                addItem(&tags, "ncn_ref", getItem(rel_tags, "ref"), 1);
            } else if (networknr == 20) {
                addItem(&tags, "lwn_ref", getItem(rel_tags, "ref"), 1);
            } else if (networknr == 21) {
                addItem(&tags, "rwn_ref", getItem(rel_tags, "ref"), 1);
            } else if (networknr == 22) {
                addItem(&tags, "nwn_ref", getItem(rel_tags, "ref"), 1);
            }
        }
    }
    else if( strcmp( type, "boundary" ) == 0 )
    {
        // Boundaries will get converted into multiple geometries:
        // - Linear features will end up in the line and roads tables (useful for admin boundaries)
        // - Polygon features also go into the polygon table (useful for national_forests)
        // The edges of the polygon also get treated as linear fetaures allowing these to be rendered seperately.
        make_boundary = 1;
    }
    else if( strcmp( type, "multipolygon" ) == 0 && getItem(&tags, "boundary") )
    {
        // Treat type=multipolygon exactly like type=boundary if it has a boundary tag.
        make_boundary = 1;
    }
    else if( strcmp( type, "multipolygon" ) == 0 )
    {
        make_polygon = 1;

        /* Copy the tags from the outer way(s) if the relation is untagged */
        /* or if there is just a name tag, people seem to like naming relations */
        if (!listHasData(&tags) || ((countList(&tags)==1) && getItem(&tags, "name"))) {
            for (i=0; xcount[i]; i++) {
                if (xrole[i] && !strcmp(xrole[i], "inner"))
                    continue;

                p = xtags[i].next;
                while (p != &(xtags[i])) {
                    addItem(&tags, p->key, p->value, 1);
                    p = p->next;
                }
            }
        }

        // Collect a list of polygon-like tags, these are used later to
        // identify if an inner rings looks like it should be rendered seperately
        p = tags.next;
        while (p != &tags) {
            if (tag_indicates_polygon(OSMTYPE_WAY, p->key)) {
                addItem(&poly_tags, p->key, p->value, 1);
                //fprintf(stderr, "found a polygon tag: %s=%s\n", p->key, p->value);
            }
            p = p->next;
        }
    }
    else
    {
        /* Unknown type, just exit */
        resetList(&tags);
        resetList(&poly_tags);
        return 0;
    }

    if (filter_tags(OSMTYPE_WAY, &tags, &polygon, Options) || add_z_order(&tags, &roads)) {
        resetList(&tags);
        resetList(&poly_tags);
        return 0;
    }

    // Split long linear ways after around 1 degree or 100km (polygons not effected)
    if (Options->projection == PROJ_LATLONG)
        split_at = 1;
    else
        split_at = 100 * 1000;

    wkt_size = build_geometry(id, xnodes, xcount, make_polygon, Options->enable_multi, split_at);

    if (!wkt_size) {
        resetList(&tags);
        resetList(&poly_tags);
        return 0;
    }

    for (i=0;i<wkt_size;i++)
    {
        char *wkt = get_wkt(i);

        if (wkt && strlen(wkt)) {
            expire_tiles_from_wkt(wkt, -id);
            /* FIXME: there should be a better way to detect polygons */
            if (!strncmp(wkt, "POLYGON", strlen("POLYGON")) || !strncmp(wkt, "MULTIPOLYGON", strlen("MULTIPOLYGON"))) {
                double area = get_area(i);
                if (area > 0.0) {
                    char tmp[32];
                    snprintf(tmp, sizeof(tmp), "%f", area);
                    addItem(&tags, "way_area", tmp, 0);
                }
                write_wkts(-id, &tags, wkt, t_poly);
            } else {
                write_wkts(-id, &tags, wkt, t_line);
                if (roads)
                    write_wkts(-id, &tags, wkt, t_roads);
            }
        }
        free(wkt);
    }

    clear_wkts();

    // If we are creating a multipolygon then we
    // mark each member so that we can skip them during iterate_ways
    // but only if the polygon-tags look the same as the outer ring
    if (make_polygon) {
        for (i=0; xcount[i]; i++) {
            int match = 0;
            struct keyval *p = poly_tags.next;
            while (p != &poly_tags) {
                const char *v = getItem(&xtags[i], p->key);
                //fprintf(stderr, "compare polygon tag: %s=%s vs %s\n", p->key, p->value, v ? v : "null");
                if (!v || strcmp(v, p->value)) {
                    match = 0;
                    break;
                }
                match = 1;
                p = p->next;
            }
            if (match) {
                //fprintf(stderr, "match for %d\n", xid[i]);
                Options->mid->ways_done(xid[i]);
                mysql_delete_way_from_output(xid[i]);
            }
        }
    }

    // If we are making a boundary then also try adding any relations which form complete rings
    // The linear variants will have already been processed above
    if (make_boundary) {
        wkt_size = build_geometry(id, xnodes, xcount, 1, Options->enable_multi, split_at);
        for (i=0;i<wkt_size;i++)
        {
            char *wkt = get_wkt(i);

            if (strlen(wkt)) {
                expire_tiles_from_wkt(wkt, -id);
                /* FIXME: there should be a better way to detect polygons */
                if (!strncmp(wkt, "POLYGON", strlen("POLYGON")) || !strncmp(wkt, "MULTIPOLYGON", strlen("MULTIPOLYGON"))) {
                    double area = get_area(i);
                    if (area > 0.0) {
                        char tmp[32];
                        snprintf(tmp, sizeof(tmp), "%f", area);
                        addItem(&tags, "way_area", tmp, 0);
                    }
                    write_wkts(-id, &tags, wkt, t_poly);
                }
            }
            free(wkt);
        }
        clear_wkts();
    }

    resetList(&tags);
    resetList(&poly_tags);
    return 0;
}

static int mysql_delete_relation_from_output(osmid_t osm_id)
{
  mysql_vexec(tables[t_roads].sql_conn, "DELETE FROM %s WHERE osm_id = %" PRIdOSMID, tables[t_roads].name, -osm_id );
  /* FIXME if ( expire_tiles_from_db(tables[t_line].sql_conn, -osm_id) != 0) */
  mysql_vexec(tables[t_line].sql_conn, "DELETE FROM %s WHERE osm_id = %" PRIdOSMID, tables[t_line].name, -osm_id );
  /* if ( expire_tiles_from_db(tables[t_poly].sql_conn, -osm_id) != 0) */
  mysql_vexec(tables[t_poly].sql_conn, "DELETE FROM %s WHERE osm_id = %" PRIdOSMID, tables[t_poly].name, -osm_id );
  return 0;
}


static int mysql_process_relation(osmid_t id, struct member *members, int member_count, struct keyval *tags, int exists)
{
  // (osmid_t id, struct keyval *rel_tags, struct osmNode **xnodes, struct keyval **xtags, int *xcount)
  int i, j, count, count2;
  osmid_t *xid2 = malloc( (member_count+1) * sizeof(osmid_t) );
  osmid_t *xid;
  const char **xrole = malloc( (member_count+1) * sizeof(const char *) );
  int *xcount = malloc( (member_count+1) * sizeof(int) );
  struct keyval *xtags  = malloc( (member_count+1) * sizeof(struct keyval) );
  struct osmNode **xnodes = malloc( (member_count+1) * sizeof(struct osmNode*) );

  /* If the flag says this object may exist already, delete it first */
  if(exists)
      mysql_delete_relation_from_output(id);

  count = 0;
  for( i=0; i<member_count; i++ )
  {
  
    /* Need to handle more than just ways... */
    if( members[i].type != OSMTYPE_WAY )
        continue;
    xid2[count] = members[i].id;
    count++;
  }

  count2 = Options->mid->ways_get_list(xid2, count, &xid, xtags, xnodes, xcount);

  for (i = 0; i < count2; i++) {
      for (j = i; j < member_count; j++) {
          if (members[j].id == xid[i]) break;
      }
      xrole[i] = members[j].role;
  }
  xnodes[count2] = NULL;
  xcount[count2] = 0;
  xid[count2] = 0;
  xrole[count2] = NULL;

  // At some point we might want to consider storing the retreived data in the members, rather than as seperate arrays
  mysql_out_relation(id, tags, xnodes, xtags, xcount, xid, xrole);

  for( i=0; i<count2; i++ )
  {
    resetList( &(xtags[i]) );
    free( xnodes[i] );
  }

  free(xid2);
  free(xid);
  free(xrole);
  free(xcount);
  free(xtags);
  free(xnodes);
  return 0;
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
  fprintf(stderr, "mysql_out_cleanup end\n");
}

static int mysql_out_start(const struct output_options *options) 
{
  char *sql, **p, tmp[256];
  size_t sql_len;
  int i, j;
  MYSQL_PARAMETERS *mysql_params;
  Options = options;

  read_style_file( options->style, options);

  sql_len = 2048;
  sql = malloc(sql_len);
  assert(sql);

  mysql_params= mysql_get_parameters();
  *mysql_params->p_max_allowed_packet= my_max_packet;

  for (i=0; i<NUM_TABLES; i++) {
    MYSQL *sql_conn = mysql_my_connect(options);
    
    tables[i].sql_conn = sql_conn;

    /* Substitute prefix into name of table */
    {
      char *temp = malloc( strlen(options->prefix) + strlen(tables[i].name) + 1 );
      sprintf( temp, tables[i].name, options->prefix );
      tables[i].name = temp;
    }

    fprintf(stderr, "Setting up table: %s\n", tables[i].name);
    
    if (!options->append) {
      mysql_vexec(sql_conn, "DROP TABLE IF EXISTS %s", tables[i].name);
      mysql_vexec(sql_conn, "DROP TABLE IF EXISTS %s_tmp", tables[i].name);
    } else { /* append */
      fprintf(stderr, "output-mysql doesn't support --append yet\n");
      exit_nicely();      
    }
    
    enum OsmType type = (i == t_point) ? OSMTYPE_NODE : OSMTYPE_WAY;
    int numTags = exportListCount[type];
    struct taginfo *exportTags = exportList[type];
    
    if (!options->append) {
      size_t needed;

      sprintf(sql, "CREATE TABLE %s ( osm_id BIGINT", tables[i].name );
      for (j=0; j < numTags; j++) {
	if( exportTags[j].flags & FLAG_DELETE )
	  continue;
	if( (exportTags[j].flags & FLAG_PHSTORE ) == FLAG_PHSTORE)
	  continue;
	sprintf(tmp, ",`%s` %s", exportTags[j].name, exportTags[j].type);
	needed = strlen(sql) + strlen(tmp) + 10;
	if (needed > sql_len) {
	  sql_len = needed;
	  sql = realloc(sql, sql_len);
	  if (!sql) {
	    fprintf(stderr, "realloc %ld failed - out of memory?\n", sql_len);
	    exit_nicely();
	  }
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

      // mysql_vexec(sql_conn, "ALTER TABLE DISABLE KEYS %s", tables[i].name);
    } else { /* append */
      fprintf(stderr, "output-mysql doesn't support --append yet\n");
      exit_nicely(); 
    }

    /* Generate column list for INSERT */
    strcpy(sql, "`osm_id`");
    tables[i].column_names = p = (char **)malloc(sizeof(char *) * (numTags+1));
    for (j=0; j < numTags; j++) {
      if( exportTags[j].flags & FLAG_DELETE )
	continue;
      if( (exportTags[j].flags & FLAG_PHSTORE ) == FLAG_PHSTORE)
	continue;

      *p++ = strdup(exportTags[j].name);

      sprintf(tmp, ",`%s`", exportTags[j].name);
      
      if (strlen(sql) + strlen(tmp) + 1 > sql_len) {
	sql_len *= 2;
	sql = realloc(sql, sql_len);
	assert(sql);
      }
      strcat(sql, tmp);
    }
    *p = NULL;

    /* FIXME hstore */
    
    tables[i].columns = strdup(sql);


    mysql_exec(sql_conn, "BEGIN");
  }  
    
  /* FIXME prepare get_wkt? */

  /* FIXME how to deal with COPY mode replacement? */

  free(sql);

  expire_tiles_init(options);
  
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
    tables[i].sql_conn = NULL;
    free(tables[i].name);
    free(tables[i].columns);
  }

  mysql_out_cleanup();
  free_style();
  
  expire_tiles_stop();
}

static int mysql_out_connect(const struct output_options *options, int startTransaction) {
  return 0;
}

static void mysql_out_close(int stopTransaction) {
  int i;
  for (i=0; i<NUM_TABLES; i++) {
    if (stopTransaction)
      mysql_exec(tables[i].sql_conn, "COMMIT");
    fprintf(stderr, "closing table %s\n", tables[i].name);
    //    mysql_vexec(tables[i].sql_conn, "ALTER TABLE ENABLE KEYS %s", tables[i].name);
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
  mysql_vexec(tables[t_point].sql_conn, "DELETE FROM %s WHERE osm_id = %"PRIdOSMID, tables[t_point].name, osm_id );
  
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
