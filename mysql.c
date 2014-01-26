#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <mysql/mysql.h>
#include "mysql.h"
#include "osmtypes.h"


int mysql_exec(MYSQL *sql_conn, const char *sql)
{
  int res;
  MYSQL_RES *result;

#ifdef DEBUG_MYSQL
  fprintf( stderr, "Executing: %s\n", sql );
#endif
  res = mysql_query(sql_conn, sql);
  if (res) {
    fprintf(stderr, "%s failed: %s\n", sql, mysql_error(sql_conn));
    exit_nicely();
  }

  result = mysql_store_result(sql_conn);  
  if (result) {
    mysql_free_result(result);
  }

  return 0;
}

int mysql_vexec(MYSQL *sql_conn, const char *fmt, ...)
{
  va_list ap;
  char *sql, *nsql;
  int n, res;
  size_t size = 1000;

  if ((sql = malloc(size)) == NULL) {
    fprintf(stderr, "Memory allocation failed\n");
    exit_nicely();
  }

  while (1) {
    /* Try to print in the allocated space. */
    va_start(ap, fmt);
    n = vsnprintf(sql, size, fmt, ap);
    va_end(ap);
    /* If that worked, return the string. */
    if (n > -1 && n < size)
      break;
    /* Else try again with more space. */
    if (n > -1)    /* glibc 2.1 */
      size = n+1; /* precisely what is needed */
    else           /* glibc 2.0 */
      size *= 2;  /* twice the old size */
    if ((nsql = realloc (sql, size)) == NULL) {
      free(sql);
      fprintf(stderr, "Memory re-allocation failed\n");
      exit_nicely();
    } else {
      sql = nsql;
    }
  }

  res = mysql_exec(sql_conn, sql);
  
  free(sql);

  return res;
}
