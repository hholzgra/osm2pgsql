#ifndef STYLE_FILE_H
#define STYLE_FILE_H

/* FIXME: Shouldn't malloc this all to begin with but call realloc()
   as required. The program will most likely segfault if it reads a
   style file with more styles than this */
#define MAX_STYLES 1000

#define FLAG_POLYGON 1    /* For polygon table */
#define FLAG_LINEAR  2    /* For lines table */
#define FLAG_NOCACHE 4    /* Optimisation: don't bother remembering this one */
#define FLAG_DELETE  8    /* These tags should be simply deleted on sight */
#define FLAG_PHSTORE 17   /* polygons without own column but listed in hstore this implies FLAG_POLYGON */
static struct flagsname {
    char *name;
    int flag;
} tagflags[] = {
    { .name = "polygon",    .flag = FLAG_POLYGON },
    { .name = "linear",     .flag = FLAG_LINEAR },
    { .name = "nocache",    .flag = FLAG_NOCACHE },
    { .name = "delete",     .flag = FLAG_DELETE },
    { .name = "phstore",    .flag = FLAG_PHSTORE }
};
#define NUM_FLAGS ((signed)(sizeof(tagflags) / sizeof(tagflags[0])))

/* Table columns, representing key= tags */
struct taginfo {
    char *name;
    char *type;
    int flags;
    int count;
};

extern struct taginfo *exportList[];
extern int exportListCount[];

void read_style_file( const char *filename, const struct output_options *Options );
void free_style_refs(const char *name, const char *type);
void free_style(void);
int tag_indicates_polygon(enum OsmType type, const char *key);

#endif
