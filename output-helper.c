#include "config.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>

#include "osmtypes.h"
#include "output-helper.h"

int add_z_order(struct keyval *tags, int *roads)
{
    const char *layer   = getItem(tags, "layer");
    const char *highway = getItem(tags, "highway");
    const char *bridge  = getItem(tags, "bridge");
    const char *tunnel  = getItem(tags, "tunnel");
    const char *railway = getItem(tags, "railway");
    const char *boundary= getItem(tags, "boundary");

    int z_order = 0;
    int l;
    unsigned int i;
    char z[13];

    l = layer ? strtol(layer, NULL, 10) : 0;
    z_order = 10 * l;
    *roads = 0;

    if (highway) {
        for (i=0; i<nLayers; i++) {
            if (!strcmp(layers[i].highway, highway)) {
                z_order += layers[i].offset;
                *roads   = layers[i].roads;
                break;
            }
        }
    }

    if (railway && strlen(railway)) {
        z_order += 5;
        *roads = 1;
    }
    // Administrative boundaries are rendered at low zooms so we prefer to use the roads table
    if (boundary && !strcmp(boundary, "administrative"))
        *roads = 1;

    if (bridge && (!strcmp(bridge, "true") || !strcmp(bridge, "yes") || !strcmp(bridge, "1")))
        z_order += 10;

    if (tunnel && (!strcmp(tunnel, "true") || !strcmp(tunnel, "yes") || !strcmp(tunnel, "1")))
        z_order -= 10;

    snprintf(z, sizeof(z), "%d", z_order);
    addItem(tags, "z_order", z, 0);

    return 0;
}

/* Append all alternate name:xx on to the name tag with space sepatators.
 * name= always comes first, the alternates are in no particular order
 * Note: A new line may be better but this does not work with Mapnik
 *
 *    <tag k="name" v="Ben Nevis" />
 *    <tag k="name:gd" v="Ben Nibheis" />
 * becomes:
 *    <tag k="name" v="Ben Nevis Ben Nibheis" />
 */
void compress_tag_name(struct keyval *tags)
{
    const char *name = getItem(tags, "name");
    struct keyval *name_ext = getMatches(tags, "name:");
    struct keyval *p;
    char out[2048];

    if (!name_ext)
        return;

    out[0] = '\0';
    if (name) {
        strncat(out, name, sizeof(out)-1);
        strncat(out, " ", sizeof(out)-1);
    }
    while((p = popItem(name_ext)) != NULL) {
        /* Exclude name:source = "dicataphone" and duplicates */
        if (strcmp(p->key, "name:source") && !strstr(out, p->value)) {
            strncat(out, p->value, sizeof(out)-1);
            strncat(out, " ", sizeof(out)-1);
        }
        freeItem(p);
    }
    free(name_ext);

    // Remove trailing space
    out[strlen(out)-1] = '\0';
    //fprintf(stderr, "*** New name: %s\n", out);
    updateItem(tags, "name", out);
}

#if 0
static void fix_motorway_shields(struct keyval *tags)
{
    const char *highway = getItem(tags, "highway");
    const char *name    = getItem(tags, "name");
    const char *ref     = getItem(tags, "ref");

    /* The current mapnik style uses ref= for motorway shields but some roads just have name= */
    if (!highway || strcmp(highway, "motorway"))
        return;

    if (name && !ref)
        addItem(tags, "ref", name, 0);
}
#endif

