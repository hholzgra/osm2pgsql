#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <mysql/mysql.h>

#include "osmtypes.h"
#include "output.h"
#include "output-mysql.h"

#define UNUSED  __attribute__ ((unused))

static void mysql_out_cleanup(void) {
}

static int mysql_out_start(const struct output_options *opt UNUSED) {
    return 0;
}

static void mysql_out_stop() {
}

static int mysql_out_connect(const struct output_options *options, int startTransaction) {
  return 0;
}

static void mysql_out_close(int stopTransaction) {
}

static int mysql_add_node(osmid_t a UNUSED, double b UNUSED, double c UNUSED, struct keyval *k UNUSED) {
  return 0;
}

static int mysql_add_way(osmid_t a UNUSED, osmid_t *b UNUSED, int c UNUSED, struct keyval *k UNUSED) {
  return 0;
}

static int mysql_add_relation(osmid_t a UNUSED, struct member *b UNUSED, int c UNUSED, struct keyval *k UNUSED) {
  return 0;
}

static int mysql_delete_node(osmid_t i UNUSED) {
  return 0;
}

static int mysql_delete_way(osmid_t i UNUSED) {
  return 0;
}

static int mysql_delete_relation(osmid_t i UNUSED) {
  return 0;
}

static int mysql_modify_node(osmid_t a UNUSED, double b UNUSED, double c UNUSED, struct keyval * k UNUSED) {
  return 0;
}

static int mysql_modify_way(osmid_t a UNUSED, osmid_t * b UNUSED, int c UNUSED, struct keyval * k UNUSED) {
  return 0;
}

static int mysql_modify_relation(osmid_t a UNUSED, struct member * b UNUSED, int c UNUSED, struct keyval * k UNUSED) {
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
