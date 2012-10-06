#ifndef OUTPUT_HELPER_H
#define OUTPUT_HELPER_H

enum table_id {
    t_point, t_line, t_poly, t_roads
};

/* Data to generate z-order column and road table
 * The name of the roads table is misleading, this table
 * is used for any feature to be shown at low zoom.
 * This includes railways and administrative boundaries too
 */
static struct {
    int offset;
    const char *highway;
    int roads;
} layers[] = {
    { 3, "minor",         0 },
    { 3, "road",          0 },
    { 3, "unclassified",  0 },
    { 3, "residential",   0 },
    { 4, "tertiary_link", 0 },
    { 4, "tertiary",      0 },
   // 5 = railway
    { 6, "secondary_link",1 },
    { 6, "secondary",     1 },
    { 7, "primary_link",  1 },
    { 7, "primary",       1 },
    { 8, "trunk_link",    1 },
    { 8, "trunk",         1 },
    { 9, "motorway_link", 1 },
    { 9, "motorway",      1 }
};
static const unsigned int nLayers = (sizeof(layers)/sizeof(*layers));

int add_z_order(struct keyval *tags, int *roads);
void compress_tag_name(struct keyval *tags);

#endif
