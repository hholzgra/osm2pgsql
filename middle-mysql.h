/* Implements the mid-layer processing for osm2pgsql
 * using several MySQL tables
 * 
 * This layer stores data read in from the planet.osm file
 * and is then read by the backend processing code to
 * emit the final geometry-enabled output formats
*/
 
#ifndef MIDDLE_MYSQL_H
#define MIDDLE_MYSQL_H

extern struct middle_t mid_mysql;

#endif
