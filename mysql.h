#ifndef MYSQL_H
#define MYSQL_H

int mysql_exec(MYSQL *sql_conn, const char *sql);
int mysql_vexec(MYSQL *sql_conn, const char *fmt, ...);

#endif
