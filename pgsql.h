/* Helper functions for pgsql access */

/* Current middle and output-pgsql do a lot of things similarly, this should
 * be used to abstract to commonalities */

//#define DEBUG_PGSQL

#include "output.h"

PGresult *pgsql_execPrepared( PGconn *sql_conn, const char *stmtName, int nParams, const char *const * paramValues, ExecStatusType expect);
int pgsql_CopyData(const char *context, PGconn *sql_conn, const char *sql);
int pgsql_exec(PGconn *sql_conn, ExecStatusType expect, const char *fmt, ...) __attribute__ ((format (printf, 3, 4)));
void escape(char *out, int len, const char *in);
const char *pgsql_conninfo(struct db_conn conn);
PGconn * pgsql_get_connection(struct db_conn conn);
int pgsql_check_version(struct db_conn conn, int version, const char *feature);
