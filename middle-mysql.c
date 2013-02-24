/* Implements the mid-layer processing for osm2pgsql
 * using several MySQL tables
 * 
 * This layer stores data read in from the planet.osm file
 * and is then read by the backend processing code to
 * emit the final geometry-enabled output formats
*/

#include "config.h"

#include <mysql.h>

#include "osmtypes.h"
#include "middle.h"
#include "middle-mysql.h"



static int my_start(const struct output_options *options)
{
  return 0;
}

static void my_stop(void) 
{
}

static void my_cleanup(void)
{
}

static void my_analyze(void)
{
}

static void my_end(void) 
{
}

static void my_commit(void) 
{
}

static int my_nodes_set(osmid_t id, double lat, double lon, struct keyval *tags) 
{
  return 0;
}

static int my_nodes_get_list(struct osmNode *out, osmid_t *nds, int nd_count)
{
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
