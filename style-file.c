#include "config.h"

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>

#include "osmtypes.h"
#include "output.h"
#include "style-file.h"

#ifndef NUM_TABLES
/* FIXME*/
#define NUM_TABLES 4
#endif

struct taginfo *exportList[NUM_TABLES]; /* Indexed by enum table_id */
int exportListCount[NUM_TABLES];

void read_style_file( const char *filename, const struct output_options *Options)
{
  FILE *in;
  int lineno = 0;
  int num_read = 0;

  exportList[OSMTYPE_NODE] = malloc( sizeof(struct taginfo) * MAX_STYLES );
  exportList[OSMTYPE_WAY]  = malloc( sizeof(struct taginfo) * MAX_STYLES );

  in = fopen( filename, "rt" );
  if( !in )
  {
    fprintf( stderr, "Couldn't open style file '%s': %s\n", filename, strerror(errno) );
    exit_nicely();
  }
  
  char buffer[1024];
  while( fgets( buffer, sizeof(buffer), in) != NULL )
  {
    lineno++;
    
    char osmtype[24];
    char tag[64];
    char datatype[24];
    char flags[128];
    int i;
    char *str;

    str = strchr( buffer, '#' );
    if( str )
      *str = '\0';
      
    int fields = sscanf( buffer, "%23s %63s %23s %127s", osmtype, tag, datatype, flags );
    if( fields <= 0 )  /* Blank line */
      continue;
    if( fields < 3 )
    {
      fprintf( stderr, "Error reading style file line %d (fields=%d)\n", lineno, fields );
      exit_nicely();
    }
    struct taginfo temp;
    temp.name = strdup(tag);
    temp.type = strdup(datatype);
    
    temp.flags = 0;
    for( str = strtok( flags, ",\r\n" ); str; str = strtok(NULL, ",\r\n") )
    {
      for( i=0; i<NUM_FLAGS; i++ )
      {
        if( strcmp( tagflags[i].name, str ) == 0 )
        {
          temp.flags |= tagflags[i].flag;
          break;
        }
      }
      if( i == NUM_FLAGS )
        fprintf( stderr, "Unknown flag '%s' line %d, ignored\n", str, lineno );
    }
    if (temp.flags==FLAG_PHSTORE) {
        if (HSTORE_NONE==(Options->enable_hstore)) {
            fprintf( stderr, "Error reading style file line %d (fields=%d)\n", lineno, fields );
            fprintf( stderr, "flag 'phstore' is invalid in non-hstore mode\n");
            exit_nicely();
        }
    }
    if ((temp.flags!=FLAG_DELETE) && ((strchr(temp.name,'?') >0) || (strchr(temp.name,'*') >0))) {
        fprintf( stderr, "wildcard '%s' in non-delete style emtry\n",temp.name);
        exit_nicely();
    }
    
    temp.count = 0;
//    printf("%s %s %d %d\n", temp.name, temp.type, temp.polygon, offset );
    
    int flag = 0;
    if( strstr( osmtype, "node" ) )
    {
      memcpy( &exportList[ OSMTYPE_NODE ][ exportListCount[ OSMTYPE_NODE ] ], &temp, sizeof(temp) );
      exportListCount[ OSMTYPE_NODE ]++;
      flag = 1;
    }
    if( strstr( osmtype, "way" ) )
    {
      memcpy( &exportList[ OSMTYPE_WAY ][ exportListCount[ OSMTYPE_WAY ] ], &temp, sizeof(temp) );
      exportListCount[ OSMTYPE_WAY ]++;
      flag = 1;
    }
    if( !flag )
    {
      fprintf( stderr, "Weird style line %d\n", lineno );
      exit_nicely();
    }
    num_read++;
  }
  if (ferror(in)) {
      perror(filename);
      exit_nicely();
  }
  if (num_read == 0) {
      fprintf(stderr, "Unable to parse any valid columns from the style file. Aborting.\n");
      exit_nicely();
  }
  fclose(in);
}

void free_style_refs(const char *name, const char *type)
{
    // Find and remove any other references to these pointers
    // This would be way easier if we kept a single list of styles
    // Currently this scales with n^2 number of styles
    int i,j;

    for (i=0; i<NUM_TABLES; i++) {
        for(j=0; j<exportListCount[i]; j++) {
            if (exportList[i][j].name == name)
                exportList[i][j].name = NULL;
            if (exportList[i][j].type == type)
                exportList[i][j].type = NULL;
        }
    }
}

void free_style(void)
{
    int i, j;
    for (i=0; i<NUM_TABLES; i++) {
        for(j=0; j<exportListCount[i]; j++) {
            free(exportList[i][j].name);
            free(exportList[i][j].type);
            free_style_refs(exportList[i][j].name, exportList[i][j].type);
        }
    }
    for (i=0; i<NUM_TABLES; i++)
        free(exportList[i]);
}

int tag_indicates_polygon(enum OsmType type, const char *key)
{
    int i;

    if (!strcmp(key, "area"))
        return 1;

    for (i=0; i < exportListCount[type]; i++) {
        if( strcmp( exportList[type][i].name, key ) == 0 )
            return exportList[type][i].flags & FLAG_POLYGON;
    }

    return 0;
}
