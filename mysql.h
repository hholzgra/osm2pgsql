#ifndef MYSQL_H
#define MYSQL_H

#include <output.h>

int mysql_exec(MYSQL *sql_conn, const char *sql);
int mysql_vexec(MYSQL *sql_conn, const char *fmt, ...);
MYSQL *mysql_my_connect(const struct output_options *options);

extern int my_max_packet;

#endif
